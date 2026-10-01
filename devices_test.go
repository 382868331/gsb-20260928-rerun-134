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

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(msg)
}

// TestCrossBatchDeviceIsExclusive proves the same scarce device is never held
// by two batches at once.
func TestCrossBatchDeviceIsExclusive(t *testing.T) {
	var active, maxActive atomic.Int32
	tracked := func(id string) Case {
		return Case{ID: id, Resources: []string{"bench"}, Run: func(context.Context, *Execution) Status {
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
	pool := NewDevicePool()
	sched1 := NewWithPool(NewRegistry(LocalAdapter([]Case{tracked("b1-a"), tracked("b1-b")})), Config{Workers: 2}, pool)
	sched2 := NewWithPool(NewRegistry(LocalAdapter([]Case{tracked("b2-a"), tracked("b2-b")})), Config{Workers: 2}, pool)

	res1 := make(chan *Result, 1)
	res2 := make(chan *Result, 1)
	go func() { r, _ := sched1.Batch(context.Background()); res1 <- r }()
	go func() { r, _ := sched2.Batch(context.Background()); res2 <- r }()

	select {
	case r := <-res1:
		if r.Counts().Passed != 2 {
			t.Fatalf("batch1: %+v", r.Counts())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("batch1 hung")
	}
	select {
	case r := <-res2:
		if r.Counts().Passed != 2 {
			t.Fatalf("batch2: %+v", r.Counts())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("batch2 hung")
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("shared device used concurrently across batches, peak=%d", got)
	}
}

// TestLostWorkerDetainsDeviceAcrossBatches shows that a lost worker keeps the
// device away from the next batch until an operator confirms the old run
// ended.
func TestLostWorkerDetainsDeviceAcrossBatches(t *testing.T) {
	releaseCase := make(chan struct{})
	cases := []Case{{ID: "vanish", Resources: []string{"rig"}, Run: func(ctx context.Context, exec *Execution) Status {
		exec.LoseWorker()
		<-releaseCase // simulate the (possibly still alive) old execution.
		return StatusPass
	}}}
	sched := NewWithPool(NewRegistry(LocalAdapter(cases)), Config{Workers: 1, RetryUncertain: 0}, NewDevicePool())
	res1, _ := sched.Batch(context.Background())
	if res1.Counts().Uncertain != 1 {
		t.Fatalf("expected uncertain, got %+v", res1.Counts())
	}
	rec, _ := res1.RecordFor("vanish")
	if len(rec.Attempts[0].Reservations) != 1 {
		t.Fatalf("uncertain attempt must keep its reservation evidence: %+v", rec.Attempts[0])
	}
	resID := rec.Attempts[0].Reservations[0]

	detained := sched.DetainedReservations()
	if len(detained) != 1 || detained[0].ID != resID || detained[0].Resource != "rig" {
		t.Fatalf("unexpected detained reservations: %+v", detained)
	}

	// A new batch competing for the same device must wait.
	done := make(chan Counts, 1)
	go func() {
		reg := NewRegistry(LocalAdapter([]Case{{ID: "next", Resources: []string{"rig"}, Run: passingRun}}))
		r, _ := sched.batch(context.Background(), reg)
		done <- r.Counts()
	}()
	select {
	case <-done:
		t.Fatal("new batch must not grab a detained device without confirmation")
	case <-time.After(50 * time.Millisecond):
	}
	stats := sched.ResourceStats()
	var rig ResourceStat
	for _, st := range stats {
		if st.Resource == "rig" {
			rig = st
		}
	}
	if !rig.Held || !rig.Detained || rig.Waiting != 1 {
		t.Fatalf("device accounting wrong while detained: %+v (all=%+v)", rig, stats)
	}

	// Operator verifies the old execution ended, records justification, and
	// releases the device.
	confirmed, ok := sched.ConfirmResourceEnded(resID, "powered off rack 3 by hand")
	if !ok {
		t.Fatal("manual confirmation rejected")
	}
	if !confirmed.Manual || confirmed.Reason != "powered off rack 3 by hand" || confirmed.ReleasedAt.IsZero() {
		t.Fatalf("confirmation evidence missing: %+v", confirmed)
	}
	close(releaseCase)

	select {
	case c := <-done:
		if c.Passed != 1 {
			t.Fatalf("new batch after release: %+v", c)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("new batch hung after confirmation")
	}

	// Ledger still contains the original uncertain reservation, now closed.
	stored, ok := sched.Reservation(resID)
	if !ok || !stored.DetainedAt.IsZero() && stored.Open() {
		t.Fatalf("reservation evidence should be retained closed: %+v ok=%v", stored, ok)
	}
	if len(sched.DetainedReservations()) != 0 {
		t.Fatal("no device should remain detained")
	}
}

// TestFairFIFOWaitingAcrossBatches mixes slow and fast cases and keeps
// arriving batches in arrival order for the same device.
func TestFairFIFOWaitingAcrossBatches(t *testing.T) {
	var mu sync.Mutex
	var order []string
	holderStarted := make(chan struct{})
	holderRelease := make(chan struct{})
	record := func(tag string) Case {
		return Case{ID: tag, Resources: []string{"desk"}, Run: func(context.Context, *Execution) Status {
			mu.Lock()
			order = append(order, tag)
			mu.Unlock()
			if tag == "holder" {
				close(holderStarted)
				<-holderRelease
			}
			return StatusPass
		}}
	}
	s := NewWithPool(NewRegistry(LocalAdapter([]Case{record("holder")})), Config{Workers: 1}, NewDevicePool())
	firstDone := make(chan struct{})
	go func() { _, _ = s.batch(context.Background(), s.registry); close(firstDone) }()
	<-holderStarted

	// Later batches (fast cases) arrive one after another and queue behind
	// the slow holder. Each arrival is given a small gap so arrival order is
	// deterministic.
	tags := []string{"fast-a", "fast-b", "fast-c"}
	var wg sync.WaitGroup
	for _, tag := range tags {
		wg.Add(1)
		tag := tag
		go func() {
			defer wg.Done()
			reg := NewRegistry(LocalAdapter([]Case{record(tag)}))
			r, _ := s.batch(context.Background(), reg)
			_ = r
		}()
		time.Sleep(10 * time.Millisecond)
	}
	close(holderRelease)
	wg.Wait()
	<-firstDone

	mu.Lock()
	defer mu.Unlock()
	want := []string{"holder", "fast-a", "fast-b", "fast-c"}
	if fmt.Sprint(order) != fmt.Sprint(want) {
		t.Fatalf("fast arrivals must run in FIFO order after slow holder: %v want %v", order, want)
	}
}

// TestCancelOneBatchKeepsDeviceForOthers ensures canceling a waiting/running
// batch releases its normally acquired devices and leaves the shared pool
// usable, while detained devices are not released.
func TestCancelOneBatchKeepsDeviceForOthers(t *testing.T) {
	cases := []Case{
		{ID: "long", Resources: []string{"unit"}, Run: func(ctx context.Context, _ *Execution) Status {
			<-ctx.Done()
			return Status{Outcome: OutcomeNotRun}
		}},
	}
	sched := NewWithPool(NewRegistry(LocalAdapter(cases)), Config{Workers: 1}, NewDevicePool())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _ = sched.Batch(ctx)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	reg := NewRegistry(LocalAdapter([]Case{{ID: "other", Resources: []string{"unit"}, Run: passingRun}}))
	r, _ := sched.batch(context.Background(), reg)
	if r.Counts().Passed != 1 {
		t.Fatalf("device must be released after normal cancel: %+v", r.Counts())
	}
}

// TestUncertainRetryNeedsManualRelease keeps the same case identity across
// retries (new attempt number and new worker) but never overwrites the first
// uncertain attempt; the retry itself waits for manual device confirmation.
func TestUncertainRetryNeedsManualRelease(t *testing.T) {
	var calls atomic.Int32
	oldRunEnded := make(chan struct{})
	cases := []Case{{ID: "same-identity", Resources: []string{"board"}, Run: func(ctx context.Context, exec *Execution) Status {
		if calls.Add(1) == 1 {
			exec.LoseWorker()
			<-oldRunEnded
			return StatusPass
		}
		return StatusPass
	}}}
	sched := NewWithPool(NewRegistry(LocalAdapter(cases)), Config{Workers: 2, RetryUncertain: 1}, NewDevicePool())
	done := make(chan *Result, 1)
	go func() { r, _ := sched.Batch(context.Background()); done <- r }()

	waitFor(t, func() bool { return len(sched.DetainedReservations()) == 1 }, "reservation not detained")
	resID := sched.DetainedReservations()[0].ID

	// The in-batch retry cannot start while the old execution might still run.
	select {
	case <-done:
		t.Fatal("batch must not finish retry while device is detained")
	case <-time.After(50 * time.Millisecond):
	}
	if _, ok := sched.ConfirmResourceEnded(resID, "operator detached board"); !ok {
		t.Fatal("confirm failed")
	}
	close(oldRunEnded)

	var r *Result
	select {
	case r = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("batch hung after confirmation")
	}
	rec, _ := r.RecordFor("same-identity")
	if rec.Case.ID != "same-identity" || rec.Final != OutcomePassed || len(rec.Attempts) != 2 {
		t.Fatalf("retry identity/history mismatch: %+v", rec)
	}
	if rec.Attempts[0].Outcome != OutcomeUncertain {
		t.Fatalf("first uncertain attempt must not be overwritten: %+v", rec.Attempts)
	}
	if rec.Attempts[0].WorkerID == rec.Attempts[1].WorkerID || rec.Attempts[1].Index != 2 {
		t.Fatalf("retry must be a new attempt on a new worker: %+v", rec.Attempts)
	}
}

// TestReportDistinguishesNotRunUncertainFailed checks the human report keeps
// the three non-success states separate and shows reservation evidence.
func TestReportDistinguishesNotRunUncertainFailed(t *testing.T) {
	release := make(chan struct{})
	cases := []Case{
		{ID: "ok", Run: passingRun},
		{ID: "bad", Run: func(context.Context, *Execution) Status {
			return Fail(errors.New("boom"))
		}},
		{ID: "gone", Resources: []string{"scope"}, Run: func(_ context.Context, exec *Execution) Status {
			exec.LoseWorker()
			<-release
			return StatusPass
		}},
	}
	sched := NewWithPool(NewRegistry(LocalAdapter(cases)), Config{Workers: 3, RetryUncertain: 0}, NewDevicePool())
	r, _ := sched.Batch(context.Background())
	report := r.Report()
	for _, want := range []string{
		"failed=1", "uncertain=1", "passed=1",
		"failed: bad", "uncertain: gone", "passed: ok",
		"reservations=",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q: %s", want, report)
		}
	}
	if strings.Contains(report, "not_run:") {
		t.Fatalf("no not_run case in this batch: %s", report)
	}
	for _, d := range sched.DetainedReservations() {
		sched.ConfirmResourceEnded(d.ID, "test cleanup")
	}
	close(release)
}

func ExampleScheduler_ConfirmResourceEnded() {
	cases := []Case{{ID: "ex", Resources: []string{"device"}, Run: func(_ context.Context, exec *Execution) Status {
		exec.LoseWorker()
		return StatusUncertain
	}}}
	sched := NewWithPool(NewRegistry(LocalAdapter(cases)), Config{Workers: 1}, NewDevicePool())
	res, _ := sched.Batch(context.Background())
	rec, _ := res.RecordFor("ex")
	id := rec.Attempts[0].Reservations[0]
	confirmed, _ := sched.ConfirmResourceEnded(id, "device physically powered off")
	fmt.Println(confirmed.Manual)
	// Output: true
}
