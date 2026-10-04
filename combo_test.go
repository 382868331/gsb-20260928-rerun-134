package testsched

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

// Fictional lab devices used by the combination-requirement demos.
const (
	devCamera   = "camera"
	devRobotArm = "robot-arm"
	devGPU      = "gpu-rig"
	devLidar    = "lidar"
)

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// scriptAdapter returns a different case list on each discovery call, so one
// scheduler can run batches with different fictional workloads.
func scriptAdapter(batches ...[]Case) Adapter {
	var call atomic.Int32
	return FuncAdapter{AdapterName: "lab", DiscoverFn: func(context.Context) ([]Case, error) {
		i := int(call.Add(1)) - 1
		if i >= len(batches) {
			i = len(batches) - 1
		}
		return batches[i], nil
	}}
}

func TestCrossingCombinationsExplainAndIndependentProgress(t *testing.T) {
	holderStarted := make(chan struct{})
	holderFinish := make(chan struct{})
	var inspectRan atomic.Bool
	mapDone := make(chan struct{})

	adapter := scriptAdapter(
		[]Case{{
			ID: "lab/weld", Resources: []string{devCamera, devRobotArm},
			Run: func(context.Context, *Execution) Status {
				close(holderStarted)
				<-holderFinish
				return StatusPass
			},
		}},
		[]Case{
			{ID: "lab/inspect", Resources: []string{devGPU, devRobotArm}, Run: func(context.Context, *Execution) Status {
				inspectRan.Store(true)
				return StatusPass
			}},
			{ID: "lab/map", Resources: []string{devLidar}, Run: func(context.Context, *Execution) Status {
				close(mapDone)
				return StatusPass
			}},
		},
	)
	sched := New(NewRegistry(adapter), Config{Workers: 4})

	firstDone := make(chan *Result, 1)
	go func() {
		r, _ := sched.Batch(context.Background())
		firstDone <- r
	}()
	<-holderStarted

	secondDone := make(chan *Result, 1)
	go func() {
		r, _ := sched.Batch(context.Background())
		secondDone <- r
	}()

	// The crossing combination waits and can explain itself.
	var first *Result
	select {
	case first = <-firstDone:
		t.Fatalf("first batch finished early: %+v", first.Counts())
	default:
	}
	eventually(t, "inspect to be queued as waiting", func() bool {
		_, ok := sched.ExplainWait("batch-2", "lab/inspect")
		return ok
	})
	ex, ok := sched.ExplainWait("batch-2", "lab/inspect")
	if !ok {
		t.Fatal("inspect should be waiting")
	}
	if !reflect.DeepEqual(ex.Required, []string{devGPU, devRobotArm}) {
		t.Fatalf("required combination mismatch: %+v", ex.Required)
	}
	if !reflect.DeepEqual(ex.Missing, []string{devRobotArm}) {
		t.Fatalf("missing capability mismatch: %+v", ex.Missing)
	}
	if ex.HeldBy[devRobotArm] != "batch-1/lab/weld" {
		t.Fatalf("holder not explained: %+v", ex.HeldBy)
	}
	// While waiting, the partial combination is not pre-granted and the case
	// is not disguised as executing.
	if inspectRan.Load() {
		t.Fatal("waiting case must not start executing")
	}
	for _, res := range sched.Reservations() {
		if res.BatchID == "batch-2" {
			t.Fatalf("waiting case must not pre-hold %s", res.Resource)
		}
	}
	// The independent lidar task still progresses.
	select {
	case <-mapDone:
	case <-time.After(2 * time.Second):
		t.Fatal("independent task starved by crossing combinations")
	}

	close(holderFinish)
	<-firstDone
	second := <-secondDone
	if got := second.Counts().Passed; got != 2 {
		t.Fatalf("second batch should pass both cases: %+v", second.Counts())
	}
	if got := sched.Reservations(); len(got) != 0 {
		t.Fatalf("reservations leaked: %+v", got)
	}
}

func TestRequirementNarrowAndCancel(t *testing.T) {
	holderStarted := make(chan struct{})
	holderFinish := make(chan struct{})

	adapter := scriptAdapter(
		[]Case{{
			ID: "lab/hold-camera", Resources: []string{devCamera},
			Run: func(context.Context, *Execution) Status {
				close(holderStarted)
				<-holderFinish
				return StatusPass
			},
		}},
		[]Case{
			{ID: "lab/combo", Resources: []string{devCamera, devRobotArm}, Run: func(context.Context, *Execution) Status {
				return StatusPass
			}},
			{ID: "lab/cancelme", Resources: []string{devCamera}, Run: func(context.Context, *Execution) Status {
				return StatusPass
			}},
		},
	)
	sched := New(NewRegistry(adapter), Config{Workers: 4})

	firstDone := make(chan *Result, 1)
	go func() {
		r, _ := sched.Batch(context.Background())
		firstDone <- r
	}()
	<-holderStarted

	secondDone := make(chan *Result, 1)
	go func() {
		r, _ := sched.Batch(context.Background())
		secondDone <- r
	}()
	eventually(t, "both requirements waiting", func() bool {
		return len(sched.Waiting()) == 2
	})

	// Widening is rejected; narrowing to the available device is accepted.
	if sched.NarrowRequirement("batch-2", "lab/combo", []string{devCamera, devGPU}) {
		t.Fatal("widening a requirement must be rejected")
	}
	if !sched.NarrowRequirement("batch-2", "lab/combo", []string{devRobotArm}) {
		t.Fatal("narrowing to a subset should succeed")
	}
	if !sched.CancelRequirement("batch-2", "lab/cancelme") {
		t.Fatal("canceling a waiting requirement should succeed")
	}

	second := <-secondDone
	rec, _ := second.RecordFor("lab/combo")
	if rec.Final != OutcomePassed {
		t.Fatalf("narrowed requirement should run on the free device: %+v", rec)
	}
	rec, _ = second.RecordFor("lab/cancelme")
	if rec.Final != OutcomeNotRun {
		t.Fatalf("canceled requirement must be not_run, got %+v", rec)
	}

	close(holderFinish)
	<-firstDone
	if got := sched.Reservations(); len(got) != 0 {
		t.Fatalf("reservations leaked: %+v", got)
	}
}

func TestSaveRestoreKeepsAccountingWithLostWorker(t *testing.T) {
	releaseRun := make(chan struct{})
	zombieGone := make(chan struct{})
	var cameraActive, cameraMax atomic.Int32
	watchCamera := func() func() {
		cur := cameraActive.Add(1)
		for {
			if old := cameraMax.Load(); cur <= old || cameraMax.CompareAndSwap(old, cur) {
				break
			}
		}
		return func() { cameraActive.Add(-1) }
	}

	adapter := scriptAdapter(
		[]Case{{
			ID: "lab/pick", Resources: []string{devCamera, devRobotArm},
			Run: func(_ context.Context, exec *Execution) Status {
				defer close(zombieGone)
				defer watchCamera()()
				exec.LoseWorker() // start response lost: worker vanishes mid-run
				<-releaseRun
				return StatusPass
			},
		}},
		[]Case{{
			ID: "lab/pick-2", Resources: []string{devCamera},
			Run: func(context.Context, *Execution) Status {
				<-zombieGone // the confirmed-ended execution is really gone
				defer watchCamera()()
				time.Sleep(10 * time.Millisecond)
				return StatusPass
			},
		}},
	)
	sched := New(NewRegistry(adapter), Config{Workers: 2, RetryUncertain: 0})

	// The first batch's worker loses its start response and keeps its holds.
	firstDone := make(chan *Result, 1)
	go func() {
		r, _ := sched.Batch(context.Background())
		firstDone <- r
	}()
	eventually(t, "uncertain holds to appear", func() bool {
		for _, r := range sched.Reservations() {
			if r.Uncertain {
				return true
			}
		}
		return false
	})

	saved := sched.SaveState()
	if len(saved.Holds) != 2 {
		t.Fatalf("expected two uncertain holds, got %+v", saved.Holds)
	}

	// Reopen: a fresh scheduler restores the exact same accounting.
	reopened := New(NewRegistry(scriptAdapter(
		[]Case{{
			ID: "lab/pick-2", Resources: []string{devCamera},
			Run: func(context.Context, *Execution) Status {
				<-zombieGone // the confirmed-ended execution is really gone
				defer watchCamera()()
				time.Sleep(10 * time.Millisecond)
				return StatusPass
			},
		}},
	)), Config{Workers: 2})
	if err := reopened.RestoreState(saved); err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	if err := reopened.RestoreState(saved); err == nil {
		t.Fatal("restoring the same holds twice must be rejected")
	}
	if !reflect.DeepEqual(reopened.Reservations(), sched.Reservations()) {
		t.Fatalf("accounting changed across reopen: old=%+v new=%+v", sched.Reservations(), reopened.Reservations())
	}

	// A competing batch on the reopened scheduler cannot take the camera
	// while the lost execution may still be running.
	blocked := make(chan *Result, 1)
	go func() {
		r, _ := reopened.Batch(context.Background())
		blocked <- r
	}()
	select {
	case r := <-blocked:
		t.Fatalf("reopened scheduler gave away a held device: %+v", r.Counts())
	case <-time.After(50 * time.Millisecond):
	}
	ex, ok := reopened.ExplainWait("batch-1", "lab/pick-2")
	if !ok || len(ex.Uncertain) != 1 || ex.Uncertain[0] != devCamera {
		t.Fatalf("wait explanation should blame the uncertain camera hold: %+v", ex)
	}

	// Human confirms the lost execution ended; the hold is released and the
	// waiting batch proceeds. No device is ever owned by two executions.
	released := reopened.ConfirmFinished("batch-1", "lab/pick")
	if len(released) != 2 {
		t.Fatalf("confirm should release both holds of the combination: %+v", released)
	}
	close(releaseRun)
	<-firstDone
	second := <-blocked
	if got := second.Counts().Passed; got != 1 {
		t.Fatalf("batch after reopen should pass: %+v", second.Counts())
	}
	if got := cameraMax.Load(); got != 1 {
		t.Fatalf("camera owned by two executions at once, peak %d", got)
	}
	if got := reopened.Reservations(); len(got) != 0 {
		t.Fatalf("reservations leaked after reopen: %+v", got)
	}
}
