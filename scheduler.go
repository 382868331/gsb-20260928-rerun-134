package testsched

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Config controls scheduling of one batch.
type Config struct {
	// Workers is the maximum number of cases running concurrently.
	Workers int
	// RetryFailed retries ordinary failed outcomes.
	RetryFailed int
	// RetryUncertain retries cases whose worker was lost.
	RetryUncertain int
}

// DefaultConfig uses four workers with one retry for uncertain results.
func DefaultConfig() Config {
	return Config{Workers: 4, RetryFailed: 0, RetryUncertain: 1}
}

// Scheduler discovers cases through adapters and runs independent batches.
// Resource reservations are shared across batches, so scarce devices stay
// mutually exclusive even when batches overlap in time.
type Scheduler struct {
	registry  *Registry
	config    Config
	resources *resourceManager
	batchSeq  atomic.Int64
}

// New creates a scheduler. A nil registry starts without adapters.
func New(registry *Registry, config Config) *Scheduler {
	if config.Workers <= 0 {
		config.Workers = 1
	}
	if config.RetryFailed < 0 {
		config.RetryFailed = 0
	}
	if config.RetryUncertain < 0 {
		config.RetryUncertain = 0
	}
	return &Scheduler{registry: registry, config: config, resources: newResourceManager()}
}

// Reservations snapshots every shared resource currently held across all
// batches, including uncertain holds left by lost workers.
func (s *Scheduler) Reservations() []Reservation { return s.resources.snapshot() }

// ConfirmFinished records a human confirmation that a previous execution of
// the case (typically one whose worker was lost) has actually ended, and
// releases any resources it still holds so waiting batches can proceed. The
// released reservations are returned as evidence; the case's recorded
// attempts, including the original uncertain one, are left untouched.
func (s *Scheduler) ConfirmFinished(batchID, caseID string) []Reservation {
	return s.resources.forceRelease(batchID, caseID)
}

// Waiting snapshots every resource request currently blocked across all
// batches, explaining for each one which resources of its combination are
// still missing, which executions hold them, and whether those holders are
// uncertain (lost workers). A waiting request holds nothing itself: a case
// only starts once its full combination can be granted atomically.
func (s *Scheduler) Waiting() []Waiter { return s.resources.waiting() }

// ShrinkWait reduces a pending resource request to a subset of its
// combination, letting a case that narrowed its needs stop waiting for
// devices it no longer requires. Only waiting requests are affected;
// devices already handed to a running execution keep their real state.
// Returns false when the case is not waiting or keep is not a non-empty
// subset of what it requested.
func (s *Scheduler) ShrinkWait(batchID, caseID string, keep []string) bool {
	return s.resources.shrink(batchID, caseID, keep)
}

// CancelWait drops a pending resource request; the blocked case finishes as
// not_run without ever holding part of its combination. Holds already
// granted to running executions are left untouched. Returns false when the
// case was not waiting.
func (s *Scheduler) CancelWait(batchID, caseID string) bool {
	return s.resources.cancelWait(batchID, caseID)
}

// SaveReservations writes the current resource ledger (including uncertain
// holds left by lost workers) as JSON, so ownership can be reconciled after
// a restart.
func (s *Scheduler) SaveReservations(w io.Writer) error {
	return json.NewEncoder(w).Encode(s.resources.snapshot())
}

// LoadReservations restores a ledger saved by SaveReservations. Restored
// holds keep their original batch/case/worker ownership and uncertain
// flags, so ConfirmFinished on the reloaded scheduler releases exactly the
// same occupations. A device already held by a different execution is never
// reassigned; conflicting entries are returned for reconciliation.
func (s *Scheduler) LoadReservations(r io.Reader) ([]Reservation, error) {
	var saved []Reservation
	if err := json.NewDecoder(r).Decode(&saved); err != nil {
		return nil, err
	}
	return s.resources.importHolds(saved), nil
}

// LedgerVersion returns the current resource ledger version. Every grant,
// release, force release, uncertain marking, shrink, cancel, or import
// advances it, so a saved version pins the exact ledger state an observation
// (such as a Simulation) was derived from.
func (s *Scheduler) LedgerVersion() uint64 { return s.resources.ledgerVersion() }

// Simulate answers a read-only what-if question about the wait queue: if
// the given holds had ended and the given waiters had shrunk, which full
// combinations could proceed and which would still be blocked by earlier
// intersecting combinations. Nothing is acquired, released, shrunk, or
// canceled; the run works on a private copy of the ledger and leaves every
// real reservation and waiter untouched. The returned Simulation records
// the ledger version it was derived from.
func (s *Scheduler) Simulate(req SimulationRequest) Simulation {
	return s.resources.simulate(req)
}

// SimulationStatus checks a saved Simulation against the current ledger.
// When the ledger has not moved since the simulation, every conclusion
// still stands. Otherwise it reports which conclusions (proceed/blocked
// verdicts for specific waiters) are affected by the observed changes and
// must be recomputed via a fresh Simulate call.
func (s *Scheduler) SimulationStatus(sim Simulation) (stale bool, recompute []string) {
	return s.resources.simulationStatus(sim)
}

// SaveSimulation writes a Simulation as JSON. The saved document carries
// the ledger version it was derived from, so a later reader can call
// SimulationStatus to learn which conclusions the ledger has invalidated.
func (s *Scheduler) SaveSimulation(w io.Writer, sim Simulation) error {
	return json.NewEncoder(w).Encode(sim)
}

// LoadSimulation reads a Simulation saved by SaveSimulation.
func (s *Scheduler) LoadSimulation(r io.Reader) (Simulation, error) {
	var sim Simulation
	if err := json.NewDecoder(r).Decode(&sim); err != nil {
		return Simulation{}, err
	}
	return sim, nil
}

// Registry exposes the configured adapters.
func (s *Scheduler) Registry() *Registry { return s.registry }

// Batch runs the currently discoverable cases under one cancellable batch.
// Canceling ctx stops only this batch: running cases are interrupted and
// recorded as not_run, undispatched cases are also not_run, and other batches
// are unaffected.
func (s *Scheduler) Batch(ctx context.Context) (*Result, []error) {
	id := fmt.Sprintf("batch-%d", s.batchSeq.Add(1))
	cases, errs := s.registry.Discover(ctx)
	records := make([]Record, len(cases))
	for i, c := range cases {
		records[i] = Record{Case: c, Final: OutcomeNotRun}
	}

	pending := make([]int, len(cases))
	for i := range cases {
		pending[i] = i
	}
	var workerSeq atomic.Int64

	for attemptNo := 1; ; attemptNo++ {
		if len(pending) == 0 || ctx.Err() != nil {
			break
		}
		next := s.runRound(ctx, id, attemptNo, cases, records, pending, s.resources, &workerSeq)
		if ctx.Err() != nil {
			break
		}
		pending = next
		if len(pending) == 0 {
			break
		}
	}

	for i := range records {
		if records[i].Final == "" {
			records[i].Final = OutcomeNotRun
		}
	}
	return &Result{BatchID: id, Records: records}, errs
}

func (s *Scheduler) runRound(
	ctx context.Context,
	batchID string,
	attemptNo int,
	cases []Case,
	records []Record,
	pending []int,
	resources *resourceManager,
	workerSeq *atomic.Int64,
) []int {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var outcomes = make([]Outcome, len(cases))
	var attempts = make([]*Attempt, len(cases))
	var retry []int
	// Each token represents one live worker. A case borrows a token; if its
	// worker is lost, it returns a freshly numbered replacement token, so
	// capacity is preserved without reusing a dead worker's identity.
	tokens := make(chan int, s.config.Workers)
	for i := 0; i < s.config.Workers; i++ {
		tokens <- int(workerSeq.Add(1))
	}

	for _, idx := range pending {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var workerID int
			select {
			case workerID = <-tokens:
			case <-ctx.Done():
				mu.Lock()
				outcomes[idx] = OutcomeNotRun
				mu.Unlock()
				return
			}
			out, att, lost := s.execute(ctx, batchID, workerID, attemptNo, cases[idx], resources)
			mu.Lock()
			outcomes[idx] = out
			attempts[idx] = att
			mu.Unlock()
			if lost {
				workerID = int(workerSeq.Add(1))
			}
			select {
			case tokens <- workerID:
			case <-ctx.Done():
			}
		}(idx)
	}
	wg.Wait()

	for _, idx := range pending {
		att := attempts[idx]
		if att == nil {
			now := time.Now()
			att = &Attempt{
				Index:     attemptNo,
				Outcome:   OutcomeNotRun,
				StartedAt: now,
				EndedAt:   now,
			}
			outcomes[idx] = OutcomeNotRun
		}
		records[idx].Attempts = append(records[idx].Attempts, *att)
		records[idx].Final = att.Outcome
	}
	if ctx.Err() != nil {
		return nil
	}
	for _, idx := range pending {
		switch outcomes[idx] {
		case OutcomeFailed:
			if failedSoFar(records[idx]) <= s.config.RetryFailed {
				retry = append(retry, idx)
			}
		case OutcomeUncertain:
			if uncertainSoFar(records[idx]) <= s.config.RetryUncertain {
				retry = append(retry, idx)
			}
		}
	}
	return retry
}

func failedSoFar(r Record) int {
	n := 0
	for _, a := range r.Attempts {
		if a.Outcome == OutcomeFailed {
			n++
		}
	}
	return n
}

func uncertainSoFar(r Record) int {
	n := 0
	for _, a := range r.Attempts {
		if a.Outcome == OutcomeUncertain {
			n++
		}
	}
	return n
}

func (s *Scheduler) execute(ctx context.Context, batchID string, workerID, attemptNo int, c Case, resources *resourceManager) (Outcome, *Attempt, bool) {
	started := time.Now()
	att := &Attempt{Index: attemptNo, WorkerID: workerID, StartedAt: started}
	release, err := resources.acquire(ctx, Reservation{BatchID: batchID, CaseID: c.ID, WorkerID: workerID}, c.Resources)
	if err != nil {
		att.Outcome = OutcomeNotRun
		att.EndedAt = time.Now()
		return OutcomeNotRun, att, false
	}

	lost := make(chan struct{})
	exec := &Execution{workerID: workerID, attempt: attemptNo, lost: lost}
	done := make(chan Status, 1)
	run := c.Run
	go func() {
		defer release()
		defer func() {
			if rec := recover(); rec != nil {
				done <- Status{Outcome: OutcomeFailed, Err: fmt.Errorf("panic: %v", rec)}
			}
		}()
		done <- run(ctx, exec)
	}()

	select {
	case <-ctx.Done():
		att.Outcome = OutcomeNotRun
	case st := <-done:
		att.Outcome = normalizeOutcome(st.Outcome)
		att.Err = st.Err
	case <-lost:
		att.Outcome = OutcomeUncertain
		att.Err = fmt.Errorf("worker %d lost contact while running %s", workerID, c.ID)
		// The run may still be executing: keep the resource held (marked
		// uncertain) until the run returns or a human confirms it ended.
		resources.markUncertain(batchID, c.ID)
	}
	att.EndedAt = time.Now()
	return att.Outcome, att, att.Outcome == OutcomeUncertain && isLost(lost)
}

func isLost(ch chan struct{}) (lost bool) {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func normalizeOutcome(o Outcome) Outcome {
	switch o {
	case OutcomePassed, OutcomeFailed, OutcomeSkipped, OutcomeUncertain, OutcomeNotRun:
		return o
	default:
		return OutcomeFailed
	}
}
