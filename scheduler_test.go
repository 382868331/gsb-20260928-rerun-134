package testsched

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func slowCase(id string, delay time.Duration) Case {
	return Case{ID: id, Name: id, Run: func(ctx context.Context, _ *Execution) Status {
		select {
		case <-time.After(delay):
			return StatusPass
		case <-ctx.Done():
			return Status{Outcome: OutcomeNotRun}
		}
	}}
}

func TestDiscoverStableIdentityAcrossAdapters(t *testing.T) {
	local := LocalAdapter([]Case{{ID: "math/add", Run: passingRun}})
	registry := NewRegistry(local)
	registry.Register(FuncAdapter{AdapterName: "remote", DiscoverFn: func(context.Context) ([]Case, error) {
		return []Case{{ID: "math/add"}, {ID: "net/ping", Run: passingRun}}, nil
	}})
	cases, errs := registry.Discover(context.Background())
	if len(errs) != 0 {
		t.Fatalf("unexpected discovery errors: %v", errs)
	}
	if len(cases) != 2 {
		t.Fatalf("expected 2 identities, got %d", len(cases))
	}
	rec, ok := findCase(cases, "math/add")
	if !ok {
		t.Fatal("shared case missing")
	}
	if len(rec.Sources) != 2 || rec.Run == nil {
		t.Fatalf("identity did not preserve executable definition and sources: %+v", rec)
	}
}

func TestBatchCountsAndParallelism(t *testing.T) {
	cases := []Case{
		slowCase("a", 30*time.Millisecond),
		slowCase("b", 30*time.Millisecond),
		slowCase("c", 30*time.Millisecond),
		slowCase("d", 30*time.Millisecond),
		{ID: "skip", Run: func(context.Context, *Execution) Status { return StatusSkip }},
		{ID: "fail", Run: func(context.Context, *Execution) Status { return Fail(errors.New("boom")) }},
	}
	sched := New(NewRegistry(LocalAdapter(cases)), Config{Workers: 4})
	start := time.Now()
	res, errs := sched.Batch(context.Background())
	elapsed := time.Since(start)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	c := res.Counts()
	if c != (Counts{Total: 6, Passed: 4, Failed: 1, Skipped: 1}) {
		t.Fatalf("unexpected counts: %+v", c)
	}
	// Four 30ms cases with four workers should overlap substantially.
	if elapsed > 80*time.Millisecond {
		t.Fatalf("cases did not run in parallel: %s", elapsed)
	}
}

func TestSharedResourcesAreExclusive(t *testing.T) {
	var active, maxActive atomic.Int32
	makeCase := func(id, resource string) Case {
		return Case{ID: id, Resources: []string{resource}, Run: func(ctx context.Context, _ *Execution) Status {
			cur := active.Add(1)
			for {
				if old := maxActive.Load(); cur <= old || maxActive.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			active.Add(-1)
			return StatusPass
		}}
	}
	cases := []Case{
		makeCase("db1", "database"),
		makeCase("db2", "database"),
		makeCase("db3", "database"),
		makeCase("fs1", "filesystem"),
		makeCase("fs2", "filesystem"),
	}
	sched := New(NewRegistry(LocalAdapter(cases)), Config{Workers: 5})
	res, _ := sched.Batch(context.Background())
	if got := res.Counts().Passed; got != 5 {
		t.Fatalf("expected 5 passes, got %d", got)
	}
	if got := maxActive.Load(); got != 2 {
		t.Fatalf("expected peak two distinct resources in use, got %d", got)
	}
}

func TestLostWorkerIsUncertainAndReplaced(t *testing.T) {
	cases := []Case{
		{ID: "lost", Run: func(ctx context.Context, exec *Execution) Status {
			time.Sleep(10 * time.Millisecond)
			exec.LoseWorker()
			return StatusPass
		}},
		slowCase("after-1", 10*time.Millisecond),
		slowCase("after-2", 10*time.Millisecond),
		slowCase("after-3", 10*time.Millisecond),
	}
	sched := New(NewRegistry(LocalAdapter(cases)), Config{Workers: 1, RetryUncertain: 0})
	res, _ := sched.Batch(context.Background())
	c := res.Counts()
	if c.Uncertain != 1 || c.Passed != 3 {
		t.Fatalf("lost worker should leave one uncertain record and still complete others: %+v", c)
	}
	rec, _ := res.RecordFor("lost")
	if rec.Final != OutcomeUncertain || len(rec.Attempts) != 1 {
		t.Fatalf("uncertain record mismatch: %+v", rec)
	}
}

func TestRetryKeepsFirstFailureHistory(t *testing.T) {
	var calls atomic.Int32
	cases := []Case{{ID: "flaky", Run: func(context.Context, *Execution) Status {
		if calls.Add(1) == 1 {
			return Fail(errors.New("first failure"))
		}
		return StatusPass
	}}}
	sched := New(NewRegistry(LocalAdapter(cases)), Config{Workers: 1, RetryFailed: 1})
	res, _ := sched.Batch(context.Background())
	rec, _ := res.RecordFor("flaky")
	if rec.Final != OutcomePassed || len(rec.Attempts) != 2 {
		t.Fatalf("expected failed then passed attempts, got %+v", rec)
	}
	if rec.Attempts[0].Outcome != OutcomeFailed || rec.Attempts[1].Outcome != OutcomePassed {
		t.Fatalf("retry erased history: %+v", rec.Attempts)
	}
	ac := res.AttemptCounts()
	if ac.Failed != 1 || ac.Passed != 1 {
		t.Fatalf("attempt totals should retain both outcomes: %+v", ac)
	}
}

func TestUncertainRetryKeepsUnknownFirstAttempt(t *testing.T) {
	var calls atomic.Int32
	cases := []Case{{ID: "vanish", Run: func(ctx context.Context, exec *Execution) Status {
		if calls.Add(1) == 1 {
			exec.LoseWorker()
			time.Sleep(5 * time.Millisecond)
			return StatusPass
		}
		return StatusPass
	}}}
	sched := New(NewRegistry(LocalAdapter(cases)), Config{Workers: 2, RetryUncertain: 1})
	res, _ := sched.Batch(context.Background())
	rec, _ := res.RecordFor("vanish")
	if rec.Final != OutcomePassed {
		t.Fatalf("expected retry pass, got %+v", rec)
	}
	if rec.Attempts[0].Outcome != OutcomeUncertain || rec.Attempts[0].WorkerID == rec.Attempts[1].WorkerID {
		t.Fatalf("uncertain attempt and replacement identity missing: %+v", rec.Attempts)
	}
}

func TestCancelStopsOnlyCurrentBatch(t *testing.T) {
	cases := []Case{
		slowCase("long-1", time.Second),
		slowCase("long-2", time.Second),
	}
	registry := NewRegistry(LocalAdapter(cases))
	sched := New(registry, Config{Workers: 2})
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var canceled *Result
	wg.Add(1)
	go func() {
		defer wg.Done()
		canceled, _ = sched.Batch(ctx)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	wg.Wait()
	if got := canceled.Counts().NotRun; got != 2 {
		t.Fatalf("canceled running cases must be not_run, got %+v", canceled.Counts())
	}

	// A later independent batch is not affected by the earlier cancellation.
	other, _ := sched.Batch(context.Background())
	if got := other.Counts().Passed; got != 2 {
		t.Fatalf("later batch should still run: %+v", other.Counts())
	}
}

func TestCancelBeforeDispatchMarksNotRun(t *testing.T) {
	cases := []Case{
		slowCase("blocked-resource-1", 30*time.Millisecond),
		slowCase("blocked-resource-2", 30*time.Millisecond),
	}
	cases[0].Resources = []string{"r"}
	cases[1].Resources = []string{"r"}
	sched := New(NewRegistry(LocalAdapter(cases)), Config{Workers: 2})
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(10*time.Millisecond, cancel)
	res, _ := sched.Batch(ctx)
	c := res.Counts()
	if c.Passed+c.NotRun != 2 || c.Failed+c.Skipped+c.Uncertain != 0 {
		t.Fatalf("canceled batch should only contain passed/not_run: %+v", c)
	}
}

func TestReportSeparatesOutcomes(t *testing.T) {
	cases := []Case{
		{ID: "ok", Run: passingRun},
		{ID: "bad", Run: func(context.Context, *Execution) Status { return StatusFail }},
		{ID: "skip", Run: func(context.Context, *Execution) Status { return StatusSkip }},
	}
	sched := New(NewRegistry(LocalAdapter(cases)), Config{Workers: 2})
	res, _ := sched.Batch(context.Background())
	report := res.Report()
	for _, want := range []string{"failed: bad", "skipped: skip", "passed: ok"} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q:\n%s", want, report)
		}
	}
}

func TestPanicIsFailure(t *testing.T) {
	cases := []Case{{ID: "panic", Run: func(context.Context, *Execution) Status {
		panic("kaboom")
	}}}
	sched := New(NewRegistry(LocalAdapter(cases)), Config{Workers: 1})
	res, _ := sched.Batch(context.Background())
	if got := res.Counts().Failed; got != 1 {
		t.Fatalf("panic should be recorded as failure: %+v", res.Counts())
	}
}

var passingRun = func(context.Context, *Execution) Status { return StatusPass }

func findCase(cases []Case, id string) (Case, bool) {
	for _, c := range cases {
		if c.ID == id {
			return c, true
		}
	}
	return Case{}, false
}

func ExampleResult_Report() {
	cases := []Case{{ID: "example", Run: func(context.Context, *Execution) Status {
		return StatusPass
	}}}
	sched := New(NewRegistry(LocalAdapter(cases)), DefaultConfig())
	res, _ := sched.Batch(context.Background())
	fmt.Println(res.Counts().Passed)
	// Output: 1
}
