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
type Scheduler struct {
	registry *Registry
	config   Config
	batchSeq atomic.Int64
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
	return &Scheduler{registry: registry, config: config}
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
	locks := newResourceLocks()
	var workerSeq atomic.Int64

	for attemptNo := 1; ; attemptNo++ {
		if len(pending) == 0 || ctx.Err() != nil {
			break
		}
		next := s.runRound(ctx, id, attemptNo, cases, records, pending, locks, &workerSeq)
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
	locks *resourceLocks,
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
			out, att, lost := s.execute(ctx, workerID, attemptNo, cases[idx], locks)
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

func (s *Scheduler) execute(ctx context.Context, workerID, attemptNo int, c Case, locks *resourceLocks) (Outcome, *Attempt, bool) {
	started := time.Now()
	att := &Attempt{Index: attemptNo, WorkerID: workerID, StartedAt: started}
	release := locks.acquire(ctx, c.Resources)
	if ctx.Err() != nil {
		release()
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
