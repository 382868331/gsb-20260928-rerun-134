package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/example/testschedule/adapter"
)

var ErrCanceled = errors.New("batch canceled")

type Worker interface {
	ID() string
	Run(context.Context, adapter.Case) (adapter.Result, bool)
}

type FunctionWorker struct {
	id string
	fn func(context.Context, adapter.Case) (adapter.Result, bool)
}

func NewWorker(id string, fn func(context.Context, adapter.Case) (adapter.Result, bool)) FunctionWorker {
	return FunctionWorker{id: id, fn: fn}
}

func (w FunctionWorker) ID() string { return w.id }

func (w FunctionWorker) Run(ctx context.Context, tc adapter.Case) (adapter.Result, bool) {
	return w.fn(ctx, tc)
}

type Attempt struct {
	Number    int
	BatchID   string
	CaseID    string
	WorkerID  string
	Status    adapter.Status
	Output    string
	Error     string
	StartedAt time.Time
	EndedAt   time.Time
}

type Counts struct {
	Passed      int
	Failed      int
	Skipped     int
	NotExecuted int
	Uncertain   int
}

type Report struct {
	BatchID string
	Counts  Counts
	Cases   map[string]Attempt
}

type Config struct {
	workerCount int
}

type Option func(*Config)

func WithWorkers(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.workerCount = n
		}
	}
}

type lockInfo struct {
	caseID   string
	batchID  string
	workerID string
}

type jobResult struct {
	index    int
	workerID string
	result   adapter.Result
	alive    bool
}

type Batch struct {
	id       string
	attempts []Attempt
}

type Scheduler struct {
	mu        sync.Mutex
	registry  *adapter.Registry
	cases     map[string]adapter.Case
	batches   map[string]*Batch
	nextBatch int
	locks     map[string]lockInfo
}

func New(registry *adapter.Registry) *Scheduler {
	return &Scheduler{
		registry: registry,
		cases:    make(map[string]adapter.Case),
		batches:  make(map[string]*Batch),
		locks:    make(map[string]lockInfo),
	}
}

func (s *Scheduler) Discover(ctx context.Context) ([]adapter.Case, error) {
	discovered, err := s.registry.Discover(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]adapter.Case, 0, len(discovered))
	for _, item := range discovered {
		tc := item.Case
		id := tc.ID()
		if existing, ok := s.cases[id]; ok && tc.Identity == "" {
			tc.Identity = existing.Identity
		}
		if tc.Identity == "" {
			tc.Identity = id
		}
		s.cases[id] = tc
		result = append(result, tc)
	}
	return result, err
}

func (s *Scheduler) Case(id string) (adapter.Case, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tc, ok := s.cases[id]
	return tc, ok
}

func (s *Scheduler) Cases() []adapter.Case {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.cases))
	for id := range s.cases {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make([]adapter.Case, 0, len(ids))
	for _, id := range ids {
		result = append(result, s.cases[id])
	}
	return result
}

func (s *Scheduler) MustAddCase(tc adapter.Case) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tc.Identity == "" {
		tc.Identity = tc.ID()
	}
	s.cases[tc.Identity] = tc
}

func (s *Scheduler) ResourceHeldBy(caseID, resource string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	owner, ok := s.locks[resource]
	return ok && owner.caseID == caseID
}

func (s *Scheduler) RunBatch(ctx context.Context, ids []string, opts ...Option) (*Report, error) {
	config := Config{workerCount: 1}
	for _, opt := range opts {
		opt(&config)
	}
	return s.RunBatchWithWorkers(ctx, ids, makeWorkers(s.registry, config.workerCount))
}

func (s *Scheduler) RunBatchWithWorkers(ctx context.Context, ids []string, workers []Worker) (*Report, error) {
	if len(workers) == 0 {
		return nil, errors.New("at least one worker is required")
	}
	s.mu.Lock()
	s.nextBatch++
	batch := &Batch{id: fmt.Sprintf("batch-%04d", s.nextBatch)}
	s.batches[batch.id] = batch
	cases := make([]adapter.Case, 0, len(ids))
	for _, id := range ids {
		tc, ok := s.cases[id]
		if !ok {
			s.mu.Unlock()
			return nil, fmt.Errorf("unknown case %q", id)
		}
		cases = append(cases, tc)
	}
	s.mu.Unlock()

	pending := make([]bool, len(cases))
	for i := range cases {
		pending[i] = true
	}

	active := 0
	finished := false
	resultsCh := make(chan jobResult, len(cases))
	done := make(chan struct{})

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return s.Report(batch.id)
		case <-ctx.Done():
			s.mu.Lock()
			for i, isPending := range pending {
				if isPending {
					pending[i] = false
					s.addAttemptLocked(batch.id, cases[i], "", adapter.StatusNotExecuted, "", ErrCanceled.Error())
				}
			}
			shouldFinish := active == 0 && !finished
			if shouldFinish {
				finished = true
			}
			s.mu.Unlock()
			if shouldFinish {
				close(done)
			}
		case jr := <-resultsCh:
			active--
			tc := cases[jr.index]
			s.mu.Lock()
			s.releaseLocksLocked(tc.ID(), jr.alive)
			if !jr.alive {
				s.holdLocksLocked(tc.ID(), batch.id, jr.workerID, tc.SortedResources())
				s.addAttemptLocked(batch.id, tc, jr.workerID, adapter.StatusUncertain, jr.result.Output, "worker lost contact")
			} else if ctx.Err() != nil {
				s.addAttemptLocked(batch.id, tc, jr.workerID, adapter.StatusNotExecuted, jr.result.Output, ErrCanceled.Error())
			} else {
				status := jr.result.Status
				if status == "" {
					status = adapter.StatusFailed
				}
				errText := ""
				if jr.result.Err != nil {
					errText = jr.result.Err.Error()
				}
				s.addAttemptLocked(batch.id, tc, jr.workerID, status, jr.result.Output, errText)
			}
			shouldFinish := active == 0 && ctx.Err() != nil && !finished
			if shouldFinish {
				finished = true
			}
			s.mu.Unlock()
			if shouldFinish {
				close(done)
			}
		case <-ticker.C:
			if ctx.Err() != nil || active == len(workers) {
				continue
			}
			s.mu.Lock()
			next := -1
			for i, isPending := range pending {
				if isPending && s.canLockLocked(cases[i]) {
					next = i
					break
				}
			}
			if next >= 0 {
				worker := workers[active%len(workers)]
				tc := cases[next]
				pending[next] = false
				if tc.Skip {
					s.addAttemptLocked(batch.id, tc, worker.ID(), adapter.StatusSkipped, tc.SkipReason, "")
				} else {
					active++
					s.holdLocksLocked(tc.ID(), batch.id, worker.ID(), tc.SortedResources())
					go func(index int, worker Worker, tc adapter.Case) {
						result, alive := worker.Run(ctx, tc)
						resultsCh <- jobResult{index: index, workerID: worker.ID(), result: result, alive: alive}
					}(next, worker, tc)
				}
			}
			shouldFinish := active == 0 && allDone(pending) && !finished
			if shouldFinish {
				finished = true
			}
			s.mu.Unlock()
			if shouldFinish {
				close(done)
			}
		}
	}
}

func makeWorkers(registry *adapter.Registry, count int) []Worker {
	if count <= 0 {
		count = 1
	}
	workers := make([]Worker, count)
	for i := range workers {
		workers[i] = NewWorker(fmt.Sprintf("worker-%d", i+1), func(ctx context.Context, tc adapter.Case) (adapter.Result, bool) {
			a, ok := registry.Get(tc.Adapter)
			if !ok {
				return adapter.Result{Status: adapter.StatusFailed, Err: fmt.Errorf("adapter %q unavailable", tc.Adapter)}, true
			}
			return a.Execute(ctx, tc), true
		})
	}
	return workers
}

func allDone(pending []bool) bool {
	for _, value := range pending {
		if value {
			return false
		}
	}
	return true
}

func (s *Scheduler) canLockLocked(tc adapter.Case) bool {
	for _, resource := range tc.SortedResources() {
		if owner, ok := s.locks[resource]; ok && owner.caseID != tc.ID() {
			return false
		}
	}
	return true
}

func (s *Scheduler) holdLocksLocked(caseID, batchID, workerID string, resources []string) {
	for _, resource := range resources {
		s.locks[resource] = lockInfo{caseID: caseID, batchID: batchID, workerID: workerID}
	}
}

func (s *Scheduler) releaseLocksLocked(caseID string, alive bool) {
	if !alive {
		return
	}
	for resource, owner := range s.locks {
		if owner.caseID == caseID {
			delete(s.locks, resource)
		}
	}
}

func (s *Scheduler) ReleaseRecoveredResources(caseID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseLocksLocked(caseID, true)
}

func (s *Scheduler) addAttemptLocked(batchID string, tc adapter.Case, workerID string, status adapter.Status, output, errText string) {
	batch := s.batches[batchID]
	now := time.Now()
	batch.attempts = append(batch.attempts, Attempt{
		Number:    len(batch.attempts) + 1,
		BatchID:   batchID,
		CaseID:    tc.ID(),
		WorkerID:  workerID,
		Status:    status,
		Output:    output,
		Error:     errText,
		StartedAt: now,
		EndedAt:   now,
	})
}

func (s *Scheduler) Report(batchID string) (*Report, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	batch, ok := s.batches[batchID]
	if !ok {
		return nil, fmt.Errorf("unknown batch %q", batchID)
	}
	report := &Report{BatchID: batchID, Cases: make(map[string]Attempt)}
	for _, attempt := range batch.attempts {
		report.Cases[attempt.CaseID] = attempt
		report.Counts = addCount(report.Counts, attempt.Status)
	}
	return report, nil
}

func addCount(counts Counts, status adapter.Status) Counts {
	switch status {
	case adapter.StatusPassed:
		counts.Passed++
	case adapter.StatusFailed:
		counts.Failed++
	case adapter.StatusSkipped:
		counts.Skipped++
	case adapter.StatusNotExecuted:
		counts.NotExecuted++
	case adapter.StatusUncertain:
		counts.Uncertain++
	}
	return counts
}

func (s *Scheduler) History(caseID string) []Attempt {
	s.mu.Lock()
	defer s.mu.Unlock()
	batchIDs := make([]string, 0, len(s.batches))
	for id := range s.batches {
		batchIDs = append(batchIDs, id)
	}
	sort.Strings(batchIDs)
	var history []Attempt
	for _, id := range batchIDs {
		for _, attempt := range s.batches[id].attempts {
			if attempt.CaseID == caseID {
				history = append(history, attempt)
			}
		}
	}
	return history
}

func (s *Scheduler) RetryFailed(ctx context.Context, batchID string, opts ...Option) (*Report, error) {
	report, err := s.Report(batchID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0)
	for id, attempt := range report.Cases {
		if attempt.Status == adapter.StatusFailed {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return s.RunBatch(ctx, ids, opts...)
}
