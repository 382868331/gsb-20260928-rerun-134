package testsched

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func waitFor(t *testing.T, what string, cond func() bool) {
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

// sharedLedger returns schedulers (one per case set) that all arbitrate on
// the same resource ledger, like overlapping batches of one scheduler.
func sharedLedger(config Config, caseSets ...[]Case) []*Scheduler {
	rm := newResourceManager()
	out := make([]*Scheduler, len(caseSets))
	for i, cases := range caseSets {
		s := New(NewRegistry(LocalAdapter(cases)), config)
		s.resources = rm
		out[i] = s
	}
	return out
}

func TestWaitingExplainsMissingHoldersAndUncertainty(t *testing.T) {
	finish := make(chan struct{})
	holder := Case{ID: "holder", Resources: []string{"d1"}, Run: func(context.Context, *Execution) Status {
		<-finish
		return StatusPass
	}}
	combo := Case{ID: "combo", Resources: []string{"d1", "d2"}, Run: func(context.Context, *Execution) Status {
		return StatusPass
	}}
	scheds := sharedLedger(Config{Workers: 1}, []Case{holder}, []Case{combo})
	holderSched, comboSched := scheds[0], scheds[1]

	holderDone := make(chan *Result, 1)
	go func() {
		r, _ := holderSched.Batch(context.Background())
		holderDone <- r
	}()
	waitFor(t, "holder reservation", func() bool {
		for _, r := range comboSched.Reservations() {
			if r.CaseID == "holder" && r.Resource == "d1" {
				return true
			}
		}
		return false
	})

	comboDone := make(chan *Result, 1)
	go func() {
		r, _ := comboSched.Batch(context.Background())
		comboDone <- r
	}()
	waitFor(t, "combo waiter", func() bool { return len(comboSched.Waiting()) == 1 })

	w := comboSched.Waiting()[0]
	if w.CaseID != "combo" {
		t.Fatalf("unexpected waiter identity: %+v", w)
	}
	if len(w.Resources) != 2 || len(w.Missing) != 1 || w.Missing[0] != "d1" {
		t.Fatalf("waiter should miss exactly d1 out of the combination: %+v", w)
	}
	if len(w.BlockedBy) != 1 || w.BlockedBy[0].CaseID != "holder" || w.BlockedBy[0].Uncertain {
		t.Fatalf("blocker should be the live holder: %+v", w.BlockedBy)
	}
	// A waiting combination must not pre-occupy any of its devices.
	for _, r := range comboSched.Reservations() {
		if r.CaseID == "combo" {
			t.Fatalf("waiting combination pre-occupied %s: %+v", r.Resource, r)
		}
	}

	// Lost holder: the explanation must show the occupation as uncertain.
	comboSched.resources.markUncertain(w.BlockedBy[0].BatchID, "holder")
	waitFor(t, "uncertain blocker", func() bool {
		ws := comboSched.Waiting()
		return len(ws) == 1 && len(ws[0].BlockedBy) == 1 && ws[0].BlockedBy[0].Uncertain
	})
	// The uncertain hold keeps blocking the combination.
	select {
	case <-comboDone:
		t.Fatal("combination acquired a device whose lost holder is unconfirmed")
	case <-time.After(30 * time.Millisecond):
	}
	released := comboSched.ConfirmFinished(w.BlockedBy[0].BatchID, "holder")
	if len(released) != 1 || !released[0].Uncertain {
		t.Fatalf("confirm should release the uncertain hold: %+v", released)
	}

	close(finish)
	var res *Result
	select {
	case res = <-comboDone:
	case <-time.After(2 * time.Second):
		t.Fatal("combination did not proceed after confirmation")
	}
	if got := res.Counts().Passed; got != 1 {
		t.Fatalf("combination should pass: %+v", res.Counts())
	}
	<-holderDone
}

func TestShrinkWaitLetsNarrowedCaseProceed(t *testing.T) {
	finish := make(chan struct{})
	holder := Case{ID: "holder", Resources: []string{"d1"}, Run: func(context.Context, *Execution) Status {
		<-finish
		return StatusPass
	}}
	comboRan := make(chan struct{})
	comboFinish := make(chan struct{})
	combo := Case{ID: "combo", Resources: []string{"d1", "d2"}, Run: func(context.Context, *Execution) Status {
		close(comboRan)
		<-comboFinish
		return StatusPass
	}}
	scheds := sharedLedger(Config{Workers: 1}, []Case{holder}, []Case{combo})
	holderSched, comboSched := scheds[0], scheds[1]

	holderDone := make(chan *Result, 1)
	go func() {
		r, _ := holderSched.Batch(context.Background())
		holderDone <- r
	}()
	waitFor(t, "holder reservation", func() bool { return len(comboSched.Reservations()) == 1 })

	comboDone := make(chan *Result, 1)
	go func() {
		r, _ := comboSched.Batch(context.Background())
		comboDone <- r
	}()
	waitFor(t, "combo waiter", func() bool { return len(comboSched.Waiting()) == 1 })
	batchID := comboSched.Waiting()[0].BatchID

	if comboSched.ShrinkWait(batchID, "combo", []string{"d1", "d3"}) {
		t.Fatal("shrink to resources outside the combination must fail")
	}
	if !comboSched.ShrinkWait(batchID, "combo", []string{"d2"}) {
		t.Fatal("shrink to a subset should succeed")
	}
	select {
	case <-comboRan:
	case <-time.After(2 * time.Second):
		t.Fatal("narrowed case did not proceed on d2 alone")
	}
	// The device already handed to the running holder is untouched.
	if held := comboSched.Reservations(); len(held) != 2 {
		t.Fatalf("holder d1 and combo d2 should both be held: %+v", held)
	}
	close(comboFinish)
	close(finish)
	<-holderDone
	res := <-comboDone
	if got := res.Counts().Passed; got != 1 {
		t.Fatalf("narrowed case should pass: %+v", res.Counts())
	}
	if got := comboSched.Reservations(); len(got) != 0 {
		t.Fatalf("reservations leaked: %+v", got)
	}
	if comboSched.ShrinkWait(batchID, "combo", []string{"d2"}) {
		t.Fatal("shrink after the wait ended must fail")
	}
}

func TestCancelWaitRecordsNotRunAndKeepsRealHolds(t *testing.T) {
	finish := make(chan struct{})
	holder := Case{ID: "holder", Resources: []string{"d1"}, Run: func(context.Context, *Execution) Status {
		<-finish
		return StatusPass
	}}
	combo := Case{ID: "combo", Resources: []string{"d1", "d2"}, Run: func(context.Context, *Execution) Status {
		return StatusPass
	}}
	scheds := sharedLedger(Config{Workers: 1}, []Case{holder}, []Case{combo})
	holderSched, comboSched := scheds[0], scheds[1]

	holderDone := make(chan *Result, 1)
	go func() {
		r, _ := holderSched.Batch(context.Background())
		holderDone <- r
	}()
	waitFor(t, "holder reservation", func() bool { return len(comboSched.Reservations()) == 1 })

	comboDone := make(chan *Result, 1)
	go func() {
		r, _ := comboSched.Batch(context.Background())
		comboDone <- r
	}()
	waitFor(t, "combo waiter", func() bool { return len(comboSched.Waiting()) == 1 })
	batchID := comboSched.Waiting()[0].BatchID

	if comboSched.CancelWait(batchID, "nobody") {
		t.Fatal("canceling an unknown waiter must fail")
	}
	if !comboSched.CancelWait(batchID, "combo") {
		t.Fatal("canceling the waiting combination should succeed")
	}
	if comboSched.CancelWait(batchID, "combo") {
		t.Fatal("repeated cancel must be a no-op")
	}
	res := <-comboDone
	rec, _ := res.RecordFor("combo")
	if rec.Final != OutcomeNotRun {
		t.Fatalf("canceled wait should be not_run, got %+v", rec)
	}
	// The holder's real occupation is unaffected and still exclusive.
	if held := comboSched.Reservations(); len(held) != 1 || held[0].CaseID != "holder" {
		t.Fatalf("holder occupation must survive the cancel: %+v", held)
	}
	close(finish)
	<-holderDone
	if got := comboSched.Reservations(); len(got) != 0 {
		t.Fatalf("reservations leaked: %+v", got)
	}
}

func TestSaveLoadReservationsReconcilesOwnership(t *testing.T) {
	releaseRun := make(chan struct{})
	var calls atomic.Int32
	zombie := Case{ID: "dev/case", Resources: []string{"device"}, Run: func(_ context.Context, exec *Execution) Status {
		if calls.Add(1) == 1 {
			exec.LoseWorker()
			<-releaseRun
			return StatusPass
		}
		return StatusPass
	}}
	sched := New(NewRegistry(LocalAdapter([]Case{zombie})), Config{Workers: 1, RetryUncertain: 0})
	first, _ := sched.Batch(context.Background())
	if got := sched.Reservations(); len(got) != 1 || !got[0].Uncertain {
		t.Fatalf("expected one uncertain hold: %+v", got)
	}

	var buf bytes.Buffer
	if err := sched.SaveReservations(&buf); err != nil {
		t.Fatalf("save: %v", err)
	}
	// Reopen: a fresh scheduler restores the exact same ledger.
	reopened := New(NewRegistry(LocalAdapter([]Case{zombie})), Config{Workers: 1, RetryUncertain: 0})
	conflicts, err := reopened.LoadReservations(bytes.NewReader(buf.Bytes()))
	if err != nil || len(conflicts) != 0 {
		t.Fatalf("load: conflicts=%+v err=%v", conflicts, err)
	}
	held := reopened.Reservations()
	if len(held) != 1 || held[0].BatchID != first.BatchID || held[0].CaseID != "dev/case" || !held[0].Uncertain {
		t.Fatalf("restored ledger lost ownership or uncertainty: %+v", held)
	}

	// The restored uncertain hold still blocks competitors on the device.
	blocked := make(chan *Result, 1)
	go func() {
		r, _ := reopened.Batch(context.Background())
		blocked <- r
	}()
	select {
	case <-blocked:
		t.Fatal("competitor acquired a device still owned by the lost execution")
	case <-time.After(50 * time.Millisecond):
	}
	// Confirming on the reopened scheduler releases the same ownership.
	released := reopened.ConfirmFinished(first.BatchID, "dev/case")
	if len(released) != 1 || !released[0].Uncertain {
		t.Fatalf("confirm after reload should release the uncertain hold: %+v", released)
	}
	if again := reopened.ConfirmFinished(first.BatchID, "dev/case"); len(again) != 0 {
		t.Fatalf("repeated confirm must not release anything twice: %+v", again)
	}
	close(releaseRun)
	select {
	case r := <-blocked:
		if got := r.Counts().Passed; got != 1 {
			t.Fatalf("competitor should pass after confirmation: %+v", r.Counts())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("competitor did not proceed after confirmation")
	}
	// The original uncertain attempt is still on record.
	if rec, _ := first.RecordFor("dev/case"); rec.Attempts[0].Outcome != OutcomeUncertain {
		t.Fatalf("save/load/confirm must not erase the lost fact: %+v", rec.Attempts)
	}

	// A device already owned by someone else is never reassigned by a load.
	other := New(NewRegistry(LocalAdapter([]Case{zombie})), Config{Workers: 1})
	other.resources.importHolds([]Reservation{{Resource: "device", BatchID: "batch-x", CaseID: "dev/case"}})
	conflicts, err = other.LoadReservations(bytes.NewReader(buf.Bytes()))
	if err != nil || len(conflicts) != 1 || conflicts[0].BatchID != first.BatchID {
		t.Fatalf("conflicting load should be reported, not applied: %+v err=%v", conflicts, err)
	}
	if got := other.Reservations(); len(got) != 1 || got[0].BatchID != "batch-x" {
		t.Fatalf("existing ownership must survive a conflicting load: %+v", got)
	}
}

func TestIntersectingCombinationsFairDisjointNotBlocked(t *testing.T) {
	startOrder := make(chan string, 8)
	holderDone := make(chan struct{})
	mk := func(id string, resources ...string) Case {
		return Case{ID: id, Resources: resources, Run: func(context.Context, *Execution) Status {
			startOrder <- id
			if id == "holder" {
				<-holderDone
			}
			return StatusPass
		}}
	}
	scheds := sharedLedger(Config{Workers: 2},
		[]Case{mk("holder", "a")},
		[]Case{mk("ab", "a", "b")},
		[]Case{mk("b-only", "b"), mk("c-only", "c")},
	)

	done := make(chan *Result, 3)
	go func() { r, _ := scheds[0].Batch(context.Background()); done <- r }()
	if got := <-startOrder; got != "holder" {
		t.Fatalf("holder should start first, got %s", got)
	}
	waitFor(t, "holder reservation", func() bool { return len(scheds[2].Reservations()) == 1 })

	// ab arrives first as a waiter for {a,b}.
	go func() { r, _ := scheds[1].Batch(context.Background()); done <- r }()
	waitFor(t, "ab waiting", func() bool {
		for _, w := range scheds[1].Waiting() {
			if w.CaseID == "ab" {
				return true
			}
		}
		return false
	})

	// b-only intersects with ab and must queue behind it; c-only is disjoint
	// and must not be blocked by the a/b contention.
	go func() { r, _ := scheds[2].Batch(context.Background()); done <- r }()
	select {
	case got := <-startOrder:
		if got != "c-only" {
			t.Fatalf("disjoint case should start first, got %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("disjoint case was blocked by unrelated contention")
	}

	close(holderDone)
	for i := 0; i < 3; i++ {
		<-done
	}
	// After the holder releases a, ab (earlier waiter) must get b before b-only.
	var order []string
	for len(startOrder) > 0 {
		order = append(order, <-startOrder)
	}
	abIdx, bIdx := -1, -1
	for i, id := range order {
		switch id {
		case "ab":
			abIdx = i
		case "b-only":
			bIdx = i
		}
	}
	if abIdx == -1 || bIdx == -1 || abIdx > bIdx {
		t.Fatalf("earlier intersecting waiter must be served first: %v", order)
	}
}

func TestWaitingSimulationIsReadOnlyAndReportsFairnessBarriers(t *testing.T) {
	rm := newResourceManager()
	rm.holds["a"] = Reservation{Resource: "a", BatchID: "batch-hold", CaseID: "holder-a", WorkerID: 1}
	rm.holds["d"] = Reservation{Resource: "d", BatchID: "batch-hold", CaseID: "holder-d", WorkerID: 1}
	rm.waiters = []resourceRequest{
		{seq: 1, batchID: "batch-1", caseID: "ab", workerID: 10, resources: []string{"a", "b"}, since: time.Now()},
		{seq: 2, batchID: "batch-1", caseID: "bc", workerID: 11, resources: []string{"b", "c"}, since: time.Now()},
		{seq: 3, batchID: "batch-1", caseID: "e-only", workerID: 12, resources: []string{"e"}, since: time.Now()},
	}
	sched := New(NewRegistry(LocalAdapter(nil)), DefaultConfig())
	sched.resources = rm

	sim, err := sched.SimulateWaiting(WaitingWhatIf{
		Finished: []ResourceOwner{{BatchID: "batch-hold", CaseID: "holder-a"}},
	})
	if err != nil {
		t.Fatalf("simulate: %v", err)
	}
	if sim.LedgerVersion != 0 {
		t.Fatalf("new ledger should record version 0, got %d", sim.LedgerVersion)
	}
	if got := casesByID(sim.Ready); !sameStringSet(got, []string{"ab", "e-only"}) {
		t.Fatalf("ready combinations = %v", got)
	}
	blocked := casesByID(sim.Blocked)
	if len(blocked) != 1 || blocked[0] != "bc" {
		t.Fatalf("bc should remain blocked, got %v", blocked)
	}
	bc := sim.Blocked[0]
	if len(bc.Missing) != 0 || len(bc.EarlierBlockers) != 1 || bc.EarlierBlockers[0].CaseID != "ab" {
		t.Fatalf("bc should be blocked only by the earlier cross-combination: %+v", bc)
	}
	if len(sim.Released) != 1 || len(sim.Released[0].Reservations) != 1 || sim.Released[0].Reservations[0].Resource != "a" {
		t.Fatalf("simulation should describe the hypothetical release: %+v", sim.Released)
	}
	if got := sched.Reservations(); len(got) != 2 {
		t.Fatalf("simulation changed live reservations: %+v", got)
	}
	live := sched.Waiting()
	if len(live) != 3 || live[0].CaseID != "ab" || !sameStringSet(live[0].Resources, []string{"a", "b"}) {
		t.Fatalf("simulation changed the live wait queue: %+v", live)
	}
}

func TestWaitingSimulationShrinkShowsWhichLaterCombinationsCanProceed(t *testing.T) {
	rm := newResourceManager()
	rm.waiters = []resourceRequest{
		{seq: 1, batchID: "batch-1", caseID: "ab", workerID: 10, resources: []string{"a", "b"}, since: time.Now()},
		{seq: 2, batchID: "batch-1", caseID: "bc", workerID: 11, resources: []string{"b", "c"}, since: time.Now()},
	}
	sched := New(NewRegistry(LocalAdapter(nil)), DefaultConfig())
	sched.resources = rm

	sim, err := sched.SimulateWaiting(WaitingWhatIf{
		Shrinks: []WaiterShrink{{BatchID: "batch-1", CaseID: "ab", Keep: []string{"a"}}},
	})
	if err != nil {
		t.Fatalf("simulate shrink: %v", err)
	}
	if got := casesByID(sim.Ready); !sameStringSet(got, []string{"ab", "bc"}) {
		t.Fatalf("shrinking ab away from b should release bc: ready=%v blocked=%+v", got, sim.Blocked)
	}
	if len(sim.Shrunk) != 1 || !sameStringSet(sim.Shrunk[0].From, []string{"a", "b"}) || !sameStringSet(sim.Shrunk[0].Keep, []string{"a"}) {
		t.Fatalf("simulation should record the hypothetical shrink: %+v", sim.Shrunk)
	}
	if !sameStringSet(sched.Waiting()[0].Resources, []string{"a", "b"}) {
		t.Fatal("simulated shrink must not modify live waiter")
	}

	if _, err := sched.SimulateWaiting(WaitingWhatIf{
		Shrinks: []WaiterShrink{{BatchID: "batch-1", CaseID: "ab", Keep: []string{"z"}}},
	}); err == nil {
		t.Fatal("shrink outside the combination must be rejected")
	}
	if _, err := sched.SimulateWaiting(WaitingWhatIf{
		Shrinks: []WaiterShrink{{BatchID: "batch-1", CaseID: "missing", Keep: []string{"a"}}},
	}); err == nil {
		t.Fatal("shrink for a non-waiter must be rejected")
	}
}

func TestSavedWaitingSimulationTracksLedgerVersion(t *testing.T) {
	rm := newResourceManager()
	rm.holds["a"] = Reservation{Resource: "a", BatchID: "batch-hold", CaseID: "holder-a", WorkerID: 1}
	rm.holds["d"] = Reservation{Resource: "d", BatchID: "batch-hold", CaseID: "holder-d", WorkerID: 1}
	rm.waiters = []resourceRequest{
		{seq: 1, batchID: "batch-1", caseID: "ab", workerID: 10, resources: []string{"a", "b"}, since: time.Now()},
		{seq: 2, batchID: "batch-1", caseID: "e-only", workerID: 12, resources: []string{"e"}, since: time.Now()},
	}
	sched := New(NewRegistry(LocalAdapter(nil)), DefaultConfig())
	sched.resources = rm

	var buf bytes.Buffer
	if err := sched.SaveWaitingSimulation(&buf, WaitingWhatIf{
		Finished: []ResourceOwner{{BatchID: "batch-hold", CaseID: "holder-a"}},
	}); err != nil {
		t.Fatalf("save simulation: %v", err)
	}
	saved, err := sched.LoadWaitingSimulation(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("load simulation: %v", err)
	}
	if saved.LedgerVersion != 0 || len(saved.Ready) != 2 {
		t.Fatalf("saved simulation lost version or conclusions: %+v", saved)
	}

	rm.importHolds([]Reservation{{Resource: "d2", BatchID: "batch-hold", CaseID: "holder-d2", WorkerID: 2}})
	fresh := sched.WaitingSimulationFreshness(saved)
	if fresh.Current || fresh.CurrentVersion != saved.LedgerVersion+1 {
		t.Fatalf("ledger change should advance past saved version: %+v", fresh)
	}
	if len(fresh.StaleConclusions) != 0 {
		t.Fatalf("unrelated d2 change should not invalidate a/b/e conclusions: %+v", fresh)
	}

	sched.ConfirmFinished("batch-hold", "holder-a")
	fresh = sched.WaitingSimulationFreshness(saved)
	if fresh.Current {
		t.Fatal("release should make simulation stale")
	}
	stale := conclusionCases(fresh.StaleConclusions)
	if len(stale) != 1 || stale[0] != "ab" {
		t.Fatalf("only ab depends on released a, got %+v", fresh)
	}
}

func casesByID(waiters []SimulatedWaiter) []string {
	out := make([]string, 0, len(waiters))
	for _, waiter := range waiters {
		out = append(out, waiter.CaseID)
	}
	return out
}

func conclusionCases(conclusions []SimulationConclusion) []string {
	out := make([]string, 0, len(conclusions))
	for _, conclusion := range conclusions {
		out = append(out, conclusion.CaseID)
	}
	return out
}
