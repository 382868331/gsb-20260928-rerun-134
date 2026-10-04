package testsched

import (
	"context"
	"fmt"
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

// Waiting explains every complete device combination that is queued but has
// not received any device yet.
func (s *Scheduler) Waiting() []WaitStatus { return s.resources.waitStatus() }

// ShrinkWaitingResources narrows a waiting combination to a subset. It never
// changes devices already delivered to a started execution.
func (s *Scheduler) ShrinkWaitingResources(batchID, caseID string, resources []string) bool {
	return s.resources.shrinkWaiting(batchID, caseID, resources)
}

// CancelWaitingResources cancels a combination before any device is granted.
// Started executions must finish, lose contact, or be explicitly confirmed.
func (s *Scheduler) CancelWaitingResources(batchID, caseID string) bool {
	return s.resources.cancelWaiting(batchID, caseID)
}

// ResourceState returns the persistent device account.
func (s *Scheduler) ResourceState() State { return s.resources.state() }

// RestoreResourceState merges a saved device account into this scheduler.
func (s *Scheduler) RestoreResourceState(state State) error {
	return s.resources.restoreState(state)
}

// SaveResourceState serializes the current device account.
func (s *Scheduler) SaveResourceState() ([]byte, error) { return s.resources.saveState() }

// LoadResourceState restores a serialized device account.
func (s *Scheduler) LoadResourceState(data []byte) error { return s.resources.loadState(data) }

// ConfirmFinished records a human confirmation that a previous execution of
// the case (typically one whose worker was lost) has actually ended, and
// releases any resources it still holds so waiting batches can proceed. The
// released reservations are returned as evidence; the case's recorded
// attempts, including the original uncertain one, are left untouched.
func (s *Scheduler) ConfirmFinished(batchID, caseID string) []Reservation {
	return s.resources.forceRelease(batchID, caseID)
}

// ConfirmExecutionFinished releases resources held by one exact execution.
func (s *Scheduler) ConfirmExecutionFinished(batchID, caseID string, attempt, workerID int) []Reservation {
	return s.resources.forceReleaseExecution(Reservation{
		BatchID:  batchID,
		CaseID:   caseID,
		Attempt:  attempt,
		WorkerID: workerID,
	})
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
	reservation := Reservation{BatchID: batchID, CaseID: c.ID, WorkerID: workerID, Attempt: attemptNo}
	release, err := resources.acquire(ctx, reservation, c.Resources)
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
		if isLost(lost) {
			att.Outcome = OutcomeUncertain
			att.Err = fmt.Errorf("worker %d lost contact while running %s", workerID, c.ID)
			resources.markUncertain(reservation)
			break
		}
		att.Outcome = OutcomeNotRun
	case st := <-done:
		att.Outcome = normalizeOutcome(st.Outcome)
		att.Err = st.Err
	case <-lost:
		att.Outcome = OutcomeUncertain
		att.Err = fmt.Errorf("worker %d lost contact while running %s", workerID, c.ID)
		// The run may still be executing: keep the resource held (marked
		// uncertain) until the run returns or a human confirms it ended.
		resources.markUncertain(reservation)
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
