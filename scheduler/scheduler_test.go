package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/example/testschedule/adapter"
	"github.com/example/testschedule/scheduler"
)

type resourceTracker struct {
	mu      sync.Mutex
	inUse   map[string]int
	maxUse  map[string]int
	running int
	maxRun  int
}

func newResourceTracker() *resourceTracker {
	return &resourceTracker{inUse: map[string]int{}, maxUse: map[string]int{}}
}

func (t *resourceTracker) acquire(resources []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.running++
	if t.running > t.maxRun {
		t.maxRun = t.running
	}
	for _, resource := range resources {
		t.inUse[resource]++
		if t.inUse[resource] > t.maxUse[resource] {
			t.maxUse[resource] = t.inUse[resource]
		}
	}
}

func (t *resourceTracker) release(resources []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.running--
	for _, resource := range resources {
		t.inUse[resource]--
	}
}

func (t *resourceTracker) maxResourceUse(resource string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.maxUse[resource]
}

func (t *resourceTracker) maxRunning() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.maxRun
}

func TestRunBatchDistributesWorkAndLocksSharedResources(t *testing.T) {
	local := adapter.NewLocalCatalog("local")
	tracker := newResourceTracker()

	addTrackedTest := func(name string, resources []string, delay time.Duration, result adapter.Result) {
		if err := local.Add(name, resources, func(context.Context) adapter.Result {
			tracker.acquire(resources)
			defer tracker.release(resources)
			time.Sleep(delay)
			return result
		}); err != nil {
			t.Fatal(err)
		}
	}

	addTrackedTest("fast", []string{"cache-a"}, 5*time.Millisecond, adapter.Result{Status: adapter.StatusPassed})
	addTrackedTest("slow-one", []string{"database"}, 25*time.Millisecond, adapter.Result{Status: adapter.StatusPassed})
	addTrackedTest("slow-two", []string{"database"}, 5*time.Millisecond, adapter.Result{Status: adapter.StatusFailed, Err: context.DeadlineExceeded})
	if err := local.AddSkipped("skipped", "unsupported platform", nil); err != nil {
		t.Fatal(err)
	}

	registry := adapter.NewRegistry()
	if err := registry.Register(local); err != nil {
		t.Fatal(err)
	}
	s := scheduler.New(registry)
	cases, err := s.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	report, err := s.RunBatch(ctx, caseIDs(cases), scheduler.WithWorkers(2))
	if err != nil {
		t.Fatal(err)
	}

	if report.Counts.Passed != 2 || report.Counts.Failed != 1 || report.Counts.Skipped != 1 {
		t.Fatalf("unexpected counts: %+v", report.Counts)
	}
	if got := tracker.maxResourceUse("database"); got != 1 {
		t.Fatalf("database was used by %d tests at once, want 1", got)
	}
	if got := tracker.maxRunning(); got != 2 {
		t.Fatalf("max running workers = %d, want 2", got)
	}
}

func TestLostWorkerRecordsUncertainAndKeepsResources(t *testing.T) {
	registry := adapter.NewRegistry()
	s := scheduler.New(registry)
	tc := adapter.Case{Adapter: "remote", Name: "unreachable", Resources: []string{"device"}, Identity: "remote::unreachable"}
	s.MustAddCase(tc)

	lost := scheduler.NewWorker("lost", func(context.Context, adapter.Case) (adapter.Result, bool) {
		return adapter.Result{}, false
	})

	report, err := s.RunBatchWithWorkers(context.Background(), []string{tc.ID()}, []scheduler.Worker{lost})
	if err != nil {
		t.Fatal(err)
	}
	if report.Counts.Uncertain != 1 {
		t.Fatalf("uncertain count = %d, want 1", report.Counts.Uncertain)
	}
	if !s.ResourceHeldBy(tc.ID(), "device") {
		t.Fatal("uncertain case resource was released")
	}
	s.ReleaseRecoveredResources(tc.ID())
	if s.ResourceHeldBy(tc.ID(), "device") {
		t.Fatal("recovered resource remains held")
	}
}

func TestCancelStopsOnlyCurrentBatch(t *testing.T) {
	local := adapter.NewLocalCatalog("local")
	releaseFirst := make(chan struct{})
	firstBatchDone := make(chan struct{})

	if err := local.Add("first", nil, func(ctx context.Context) adapter.Result {
		select {
		case <-releaseFirst:
			return adapter.Result{Status: adapter.StatusPassed}
		case <-firstBatchDone:
		case <-ctx.Done():
		}
		if ctx.Err() == nil {
			return adapter.Result{Status: adapter.StatusPassed}
		}
		return adapter.Result{Status: adapter.StatusPassed}
	}); err != nil {
		t.Fatal(err)
	}
	if err := local.Add("second", nil, func(ctx context.Context) adapter.Result {
		select {
		case <-ctx.Done():
		case <-firstBatchDone:
		}
		return adapter.Result{Status: adapter.StatusPassed}
	}); err != nil {
		t.Fatal(err)
	}

	registry := adapter.NewRegistry()
	if err := registry.Register(local); err != nil {
		t.Fatal(err)
	}
	s := scheduler.New(registry)
	if _, err := s.Discover(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *scheduler.Report)
	go func() {
		report, _ := s.RunBatch(ctx, []string{"local::first", "local::second"})
		done <- report
	}()

	cancel()
	close(releaseFirst)
	report := <-done
	close(firstBatchDone)

	if report.Counts.Passed != 0 || report.Counts.NotExecuted != 2 {
		t.Fatalf("canceled batch counts = %+v", report.Counts)
	}

	after, err := s.RunBatch(context.Background(), []string{"local::first", "local::second"}, scheduler.WithWorkers(2))
	if err != nil {
		t.Fatal(err)
	}
	if after.Counts.Passed != 2 {
		t.Fatalf("later batch was affected by cancel: %+v", after.Counts)
	}
}

func TestRetryKeepsFirstFailureHistory(t *testing.T) {
	local := adapter.NewLocalCatalog("local")
	attempts := 0
	mu := sync.Mutex{}
	if err := local.Add("flaky", nil, func(context.Context) adapter.Result {
		mu.Lock()
		attempts++
		current := attempts
		mu.Unlock()
		if current == 1 {
			return adapter.Result{Status: adapter.StatusFailed, Err: context.DeadlineExceeded}
		}
		return adapter.Result{Status: adapter.StatusPassed}
	}); err != nil {
		t.Fatal(err)
	}
	registry := adapter.NewRegistry()
	if err := registry.Register(local); err != nil {
		t.Fatal(err)
	}
	s := scheduler.New(registry)
	if _, err := s.Discover(context.Background()); err != nil {
		t.Fatal(err)
	}

	first, err := s.RunBatch(context.Background(), []string{"local::flaky"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.RetryFailed(context.Background(), first.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	if second.Counts.Passed != 1 {
		t.Fatalf("retry counts = %+v", second.Counts)
	}
	history := s.History("local::flaky")
	if len(history) != 2 || history[0].Status != adapter.StatusFailed || history[1].Status != adapter.StatusPassed {
		t.Fatalf("history was rewritten: %+v", history)
	}
}

func TestLaterAdapterKeepsStableIdentity(t *testing.T) {
	s := scheduler.New(adapter.NewRegistry())
	first := adapter.Case{Adapter: "local", Name: "test", Identity: "suite::test", Resources: []string{"old"}}
	second := adapter.Case{Adapter: "remote", Name: "remote-test", Identity: "suite::test", Resources: []string{"new"}}
	s.MustAddCase(first)
	s.MustAddCase(second)

	tc, ok := s.Case("suite::test")
	if !ok || tc.Adapter != "remote" || tc.Name != "remote-test" {
		t.Fatalf("stable identity was not updated: %+v", tc)
	}
	if history := s.History("suite::test"); history != nil {
		t.Fatalf("new discovery should not create execution history: %+v", history)
	}
}

func caseIDs(cases []adapter.Case) []string {
	ids := make([]string, len(cases))
	for i, tc := range cases {
		ids[i] = tc.ID()
	}
	return ids
}
