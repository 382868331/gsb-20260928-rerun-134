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

func TestCrossBatchResourceMutualExclusion(t *testing.T) {
	var active, maxActive atomic.Int32
	mk := func(id string) Case {
		return Case{ID: id, Resources: []string{"device"}, Run: func(context.Context, *Execution) Status {
			cur := active.Add(1)
			for {
				if old := maxActive.Load(); cur <= old || maxActive.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(15 * time.Millisecond)
			active.Add(-1)
			return StatusPass
		}}
	}
	sched := New(NewRegistry(LocalAdapter([]Case{mk("dev/a"), mk("dev/b")})), Config{Workers: 2})
	var wg sync.WaitGroup
	results := make([]*Result, 3)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, _ := sched.Batch(context.Background())
			results[i] = r
		}(i)
	}
	wg.Wait()
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("device double-booked across batches, peak holders: %d", got)
	}
	for i, r := range results {
		if got := r.Counts().Passed; got != 2 {
			t.Fatalf("batch %d should pass both cases: %+v", i, r.Counts())
		}
	}
	if got := sched.Reservations(); len(got) != 0 {
		t.Fatalf("reservations leaked after batches finished: %+v", got)
	}
}

func TestLostWorkerKeepsResourceUntilConfirmed(t *testing.T) {
	releaseRun := make(chan struct{})
	var calls atomic.Int32
	zombie := Case{ID: "dev/case", Resources: []string{"device"}, Run: func(_ context.Context, exec *Execution) Status {
		if calls.Add(1) == 1 {
			exec.LoseWorker()
			<-releaseRun // lost worker: the case may still be executing
			return StatusPass
		}
		return StatusPass
	}}
	sched := New(NewRegistry(LocalAdapter([]Case{zombie})), Config{Workers: 1, RetryUncertain: 0})

	first, _ := sched.Batch(context.Background())
	rec, _ := first.RecordFor("dev/case")
	if rec.Final != OutcomeUncertain || len(rec.Attempts) != 1 {
		t.Fatalf("first batch should record one uncertain attempt: %+v", rec)
	}
	held := sched.Reservations()
	if len(held) != 1 || held[0].Resource != "device" || !held[0].Uncertain {
		t.Fatalf("lost worker must keep an uncertain device hold: %+v", held)
	}

	// A new batch competing for the same device must not acquire it yet.
	done := make(chan *Result, 1)
	go func() {
		r, _ := sched.Batch(context.Background())
		done <- r
	}()
	select {
	case <-done:
		t.Fatal("next batch acquired the device while the lost execution may still run")
	case <-time.After(50 * time.Millisecond):
	}

	// Human confirms the old execution ended; the hold is released with
	// evidence returned, and the original uncertain attempt is preserved.
	released := sched.ConfirmFinished(first.BatchID, "dev/case")
	if len(released) != 1 || released[0].Resource != "device" || !released[0].Uncertain {
		t.Fatalf("confirm should release the uncertain device hold: %+v", released)
	}
	close(releaseRun)
	var second *Result
	select {
	case second = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("next batch did not proceed after confirmation")
	}
	if got := second.Counts().Passed; got != 1 {
		t.Fatalf("next batch should pass after release: %+v", second.Counts())
	}
	if got := sched.Reservations(); len(got) != 0 {
		t.Fatalf("reservations leaked after confirmation: %+v", got)
	}
	rec, _ = first.RecordFor("dev/case")
	if rec.Attempts[0].Outcome != OutcomeUncertain {
		t.Fatalf("confirmation must not rewrite the uncertain attempt: %+v", rec.Attempts)
	}
}

func TestFairnessAndAccountingWithMixedDurations(t *testing.T) {
	var active, maxActive atomic.Int32
	mk := func(id string, d time.Duration) Case {
		return Case{ID: id, Resources: []string{"device"}, Run: func(ctx context.Context, _ *Execution) Status {
			cur := active.Add(1)
			for {
				if old := maxActive.Load(); cur <= old || maxActive.CompareAndSwap(old, cur) {
					break
				}
			}
			defer active.Add(-1)
			select {
			case <-time.After(d):
				return StatusPass
			case <-ctx.Done():
				return Status{Outcome: OutcomeNotRun}
			}
		}}
	}
	cases := []Case{
		mk("fast-1", time.Millisecond),
		mk("fast-2", time.Millisecond),
		mk("fast-3", time.Millisecond),
		mk("slow-1", 30*time.Millisecond),
	}
	sched := New(NewRegistry(LocalAdapter(cases)), Config{Workers: 4})

	results := make([]*Result, 4)
	var wg sync.WaitGroup
	// One batch is canceled mid-flight.
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		r, _ := sched.Batch(ctx)
		results[0] = r
	}()
	time.Sleep(5 * time.Millisecond)
	cancel()
	// New batches keep arriving while the canceled one unwinds.
	for i := 1; i < len(results); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, _ := sched.Batch(context.Background())
			results[i] = r
		}(i)
	}
	wg.Wait()

	if got := maxActive.Load(); got != 1 {
		t.Fatalf("device double-booked under churn, peak holders: %d", got)
	}
	if got := sched.Reservations(); len(got) != 0 {
		t.Fatalf("reservations leaked under churn: %+v", got)
	}
	for i, r := range results {
		c := r.Counts()
		if c.Total != len(cases) || c.Failed != 0 || c.Uncertain != 0 || c.Skipped != 0 {
			t.Fatalf("batch %d has impossible counts: %+v", i, c)
		}
		if c.Passed+c.NotRun != c.Total {
			t.Fatalf("batch %d counts do not add up: %+v", i, c)
		}
		if i > 0 && c.Passed != len(cases) {
			t.Fatalf("batch %d should pass every case: %+v", i, c)
		}
	}
	// The canceled batch must distinguish interrupted cases from failures.
	if results[0].Counts().NotRun == 0 {
		t.Fatalf("canceled batch should record not_run cases: %+v", results[0].Counts())
	}
}

func TestOverlappingCombinationsBlockWhileDisjointCombinationsProceed(t *testing.T) {
	manager := newResourceManager()
	holder := Reservation{BatchID: "batch-1", CaseID: "holder", WorkerID: 1, Attempt: 1}
	holdRelease, err := manager.acquire(context.Background(), holder, []string{"a", "b"})
	if err != nil {
		t.Fatalf("initial combination not granted: %v", err)
	}

	overlappingStarted := make(chan struct{})
	go func() {
		release, err := manager.acquire(context.Background(), Reservation{BatchID: "batch-2", CaseID: "overlap", WorkerID: 2, Attempt: 1}, []string{"b", "c"})
		if err == nil {
			release()
		}
		close(overlappingStarted)
	}()
	time.Sleep(10 * time.Millisecond)

	disjointDone := make(chan struct{})
	go func() {
		defer close(disjointDone)
		release, err := manager.acquire(context.Background(), Reservation{BatchID: "batch-3", CaseID: "disjoint", WorkerID: 3, Attempt: 1}, []string{"d", "e"})
		if err != nil {
			t.Errorf("disjoint combination should not be blocked: %v", err)
			return
		}
		release()
	}()
	select {
	case <-disjointDone:
	case <-time.After(time.Second):
		t.Fatal("disjoint combination was blocked by an unrelated waiting combination")
	}
	select {
	case <-overlappingStarted:
		t.Fatal("overlapping combination started before the holder released devices")
	default:
	}

	waiting := manager.waitStatus()
	if len(waiting) != 1 || waiting[0].CaseID != "overlap" {
		t.Fatalf("unexpected waiters: %+v", waiting)
	}
	if len(waiting[0].Missing) != 1 || waiting[0].Missing[0].Resource != "b" || waiting[0].Missing[0].Holder.CaseID != "holder" {
		t.Fatalf("wait reason must identify the missing device and holder: %+v", waiting[0].Missing)
	}
	if holds := manager.snapshot(); len(holds) != 2 {
		t.Fatalf("waiting combination must not pre-hold devices: %+v", holds)
	}

	holdRelease()
	select {
	case <-overlappingStarted:
	case <-time.After(time.Second):
		t.Fatal("overlapping combination did not proceed after devices became available")
	}
}

func TestShrinkAndCancelOnlyAffectWaitingCombinations(t *testing.T) {
	manager := newResourceManager()
	holderRelease, _ := manager.acquire(context.Background(), Reservation{BatchID: "batch-1", CaseID: "holder", WorkerID: 1, Attempt: 1}, []string{"a"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	granted := make(chan func(), 1)
	go func() {
		release, err := manager.acquire(ctx, Reservation{BatchID: "batch-2", CaseID: "waiting", WorkerID: 2, Attempt: 1}, []string{"a", "b"})
		if err == nil {
			granted <- release
		}
	}()
	time.Sleep(10 * time.Millisecond)

	if manager.shrinkWaiting("batch-2", "waiting", []string{"c"}) {
		t.Fatal("shrink accepted a resource outside the original combination")
	}
	if !manager.shrinkWaiting("batch-2", "waiting", []string{"b"}) {
		t.Fatal("valid shrink was rejected")
	}
	holderRelease()
	var release func()
	select {
	case release = <-granted:
	case <-time.After(time.Second):
		t.Fatal("shrunk combination did not proceed")
	}
	holds := manager.snapshot()
	if len(holds) != 1 || holds[0].Resource != "b" || holds[0].CaseID != "waiting" {
		t.Fatalf("only the shrunken combination should be held: %+v", holds)
	}
	if manager.cancelWaiting("batch-2", "waiting") {
		t.Fatal("cancel must not remove a device already delivered to an execution")
	}
	release()

	holder, _ := manager.acquire(context.Background(), Reservation{BatchID: "batch-3", CaseID: "holder", WorkerID: 3, Attempt: 1}, []string{"x"})
	go func() {
		_, _ = manager.acquire(ctx, Reservation{BatchID: "batch-4", CaseID: "queued", WorkerID: 4, Attempt: 1}, []string{"x"})
	}()
	time.Sleep(10 * time.Millisecond)
	if !manager.cancelWaiting("batch-4", "queued") {
		t.Fatal("waiting request could not be canceled")
	}
	time.Sleep(10 * time.Millisecond)
	if waiting := manager.waitStatus(); len(waiting) != 0 {
		t.Fatalf("canceled waiter remains visible: %+v", waiting)
	}
	holder()
}

func TestResourceStatePersistsLostOwnership(t *testing.T) {
	manager := newResourceManager()
	reservation := Reservation{BatchID: "batch-lost", CaseID: "case", WorkerID: 7, Attempt: 2, Uncertain: true}
	release, _ := manager.acquire(context.Background(), reservation, []string{"phone", "watch"})
	manager.markUncertain(reservation)
	data, err := manager.saveState()
	if err != nil {
		t.Fatalf("save state: %v", err)
	}

	restored := newResourceManager()
	if err := restored.loadState(data); err != nil {
		t.Fatalf("load state: %v", err)
	}
	holds := restored.snapshot()
	if len(holds) != 2 {
		t.Fatalf("expected both devices restored, got %+v", holds)
	}
	for _, hold := range holds {
		if hold.BatchID != "batch-lost" || hold.CaseID != "case" || hold.WorkerID != 7 || hold.Attempt != 2 || !hold.Uncertain || hold.Since.IsZero() {
			t.Fatalf("lost ownership was not preserved: %+v", hold)
		}
	}
	if err := restored.loadState(data); err == nil {
		t.Fatal("loading the same devices again must not create double ownership")
	}

	done := make(chan struct{})
	go func() {
		r, err := restored.acquire(context.Background(), Reservation{BatchID: "new", CaseID: "new", WorkerID: 1, Attempt: 1}, []string{"phone"})
		if err == nil {
			r()
		}
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("restored uncertain hold was reusable without confirmation")
	case <-time.After(50 * time.Millisecond):
	}
	released := restored.forceReleaseExecution(Reservation{BatchID: "batch-lost", CaseID: "case", WorkerID: 7, Attempt: 2})
	if len(released) != 2 {
		t.Fatalf("confirmation should release the exact restored execution: %+v", released)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("new execution did not proceed after restoring and confirming")
	}
	release()
}

func TestLateReleaseAndRepeatConfirmationTargetExactExecution(t *testing.T) {
	manager := newResourceManager()
	old := Reservation{BatchID: "batch", CaseID: "case", WorkerID: 1, Attempt: 1}
	oldRelease, _ := manager.acquire(context.Background(), old, []string{"device"})
	manager.markUncertain(old)

	manager.forceReleaseExecution(old)
	new := Reservation{BatchID: "batch", CaseID: "case", WorkerID: 2, Attempt: 2}
	newRelease, _ := manager.acquire(context.Background(), new, []string{"device"})
	if again := manager.forceReleaseExecution(old); len(again) != 0 {
		t.Fatalf("repeat confirmation released the new execution: %+v", again)
	}
	oldRelease()
	holds := manager.snapshot()
	if len(holds) != 1 || holds[0].WorkerID != 2 || holds[0].Attempt != 2 {
		t.Fatalf("old execution erased or stole the new hold: %+v", holds)
	}
	newRelease()
}
