package testsched

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// crossingQueue starts overlapping batches on one shared ledger: "holder"
// occupies d1, then comboA {d1,d2} and comboB {d2,d3} queue behind it in
// that arrival order. It returns the scheduler, the holder's batch ID, and
// a finish function that releases every batch and waits for them to end.
func crossingQueue(t *testing.T) (*Scheduler, string, func()) {
	t.Helper()
	finish := make(chan struct{})
	release := make(chan struct{})
	holder := Case{ID: "holder", Resources: []string{"d1"}, Run: func(context.Context, *Execution) Status {
		<-finish
		return StatusPass
	}}
	comboA := Case{ID: "comboA", Resources: []string{"d1", "d2"}, Run: func(context.Context, *Execution) Status {
		<-release
		return StatusPass
	}}
	comboB := Case{ID: "comboB", Resources: []string{"d2", "d3"}, Run: func(context.Context, *Execution) Status {
		return StatusPass
	}}
	scheds := sharedLedger(Config{Workers: 1}, []Case{holder}, []Case{comboA}, []Case{comboB})
	s := scheds[0]
	dones := make([]chan *Result, len(scheds))
	for i, sched := range scheds {
		dones[i] = make(chan *Result, 1)
		go func(sched *Scheduler, done chan *Result) {
			r, _ := sched.Batch(context.Background())
			done <- r
		}(sched, dones[i])
		if i == 0 {
			waitFor(t, "holder reservation", func() bool { return len(s.Reservations()) == 1 })
		} else {
			waitFor(t, "waiter to queue", func() bool { return len(s.Waiting()) == i })
		}
	}
	return s, "batch-1", func() {
		close(finish)
		waitFor(t, "comboA to acquire", func() bool {
			for _, r := range s.Reservations() {
				if r.CaseID == "comboA" {
					return true
				}
			}
			return false
		})
		close(release)
		for _, done := range dones {
			<-done
		}
	}
}

func blockedVerdict(t *testing.T, sim Simulation, caseID string) SimulatedWaiter {
	t.Helper()
	for _, w := range sim.Blocked {
		if w.CaseID == caseID {
			return w
		}
	}
	t.Fatalf("expected %s among blocked waiters, got %+v", caseID, sim.Blocked)
	return SimulatedWaiter{}
}

func proceedVerdict(t *testing.T, sim Simulation, caseID string) SimulatedWaiter {
	t.Helper()
	for _, w := range sim.Proceed {
		if w.CaseID == caseID {
			return w
		}
	}
	t.Fatalf("expected %s among proceeding waiters, got %+v", caseID, sim.Proceed)
	return SimulatedWaiter{}
}

func TestSimulateBaselineShowsHoldsAndFairness(t *testing.T) {
	s, batchID, finish := crossingQueue(t)
	defer finish()

	sim := s.Simulate(SimulationRequest{})
	if len(sim.Proceed) != 0 {
		t.Fatalf("no waiter can proceed while holder occupies d1: %+v", sim.Proceed)
	}
	a := blockedVerdict(t, sim, "comboA")
	if len(a.BlockedByHolds) != 1 || a.BlockedByHolds[0].Resource != "d1" ||
		a.BlockedByHolds[0].BatchID != batchID || a.BlockedByHolds[0].CaseID != "holder" {
		t.Fatalf("comboA should be blocked by holder's d1 reservation: %+v", a.BlockedByHolds)
	}
	b := blockedVerdict(t, sim, "comboB")
	if len(b.BlockedByHolds) != 0 {
		t.Fatalf("comboB resources are free; only fairness may block it: %+v", b.BlockedByHolds)
	}
	if len(b.BlockedByWaiters) != 1 || b.BlockedByWaiters[0] != batchID+"/comboA" {
		t.Fatalf("comboB should yield to earlier intersecting comboA: %+v", b.BlockedByWaiters)
	}
}

func TestSimulateReleaseAndShrinkAreWhatIfOnly(t *testing.T) {
	s, batchID, finish := crossingQueue(t)
	defer finish()

	versionBefore := s.LedgerVersion()
	holdsBefore := s.Reservations()
	waitingBefore := s.Waiting()

	// Hypothetical: holder's occupation ends. comboA proceeds and, by
	// arriving first, keeps comboB waiting on the shared d2.
	sim := s.Simulate(SimulationRequest{ReleaseOwners: []Owner{{BatchID: batchID, CaseID: "holder"}}})
	proceedVerdict(t, sim, "comboA")
	b := blockedVerdict(t, sim, "comboB")
	if len(b.BlockedByHolds) != 1 || b.BlockedByHolds[0].Resource != "d2" ||
		b.BlockedByHolds[0].CaseID != "comboA" {
		t.Fatalf("after release comboB should be blocked by comboA's simulated d2 hold: %+v", b.BlockedByHolds)
	}

	// Hypothetical: holder ends AND comboA shrinks to {d1}. The crossing
	// combinations no longer intersect, so both proceed.
	sim = s.Simulate(SimulationRequest{
		ReleaseOwners: []Owner{{BatchID: batchID, CaseID: "holder"}},
		Shrinks:       []SimulatedShrink{{BatchID: batchID, CaseID: "comboA", Keep: []string{"d1"}}},
	})
	a := proceedVerdict(t, sim, "comboA")
	if len(a.Resources) != 1 || a.Resources[0] != "d1" {
		t.Fatalf("shrunk comboA should keep only d1: %+v", a.Resources)
	}
	proceedVerdict(t, sim, "comboB")

	// Nothing actually changed: version, holds, and waiters are untouched.
	if got := s.LedgerVersion(); got != versionBefore {
		t.Fatalf("simulation must not advance the ledger version: %d -> %d", versionBefore, got)
	}
	if got := s.Reservations(); len(got) != len(holdsBefore) {
		t.Fatalf("simulation must not release holds: %+v", got)
	}
	if got := s.Waiting(); len(got) != len(waitingBefore) {
		t.Fatalf("simulation must not shrink waiters: %+v", got)
	}
	for _, w := range s.Waiting() {
		if w.CaseID == "comboA" && len(w.Resources) != 2 {
			t.Fatalf("comboA must still wait for its full combination: %+v", w.Resources)
		}
	}
}

func TestSimulationStatusFlagsStaleConclusions(t *testing.T) {
	s, batchID, finish := crossingQueue(t)
	defer finish()

	sim := s.Simulate(SimulationRequest{ReleaseOwners: []Owner{{BatchID: batchID, CaseID: "holder"}}})

	// Persist and reload: the saved simulation keeps its ledger version.
	var buf bytes.Buffer
	if err := s.SaveSimulation(&buf, sim); err != nil {
		t.Fatalf("save simulation: %v", err)
	}
	loaded, err := s.LoadSimulation(&buf)
	if err != nil {
		t.Fatalf("load simulation: %v", err)
	}
	if loaded.Version != sim.Version {
		t.Fatalf("saved simulation must carry the ledger version: %d != %d", loaded.Version, sim.Version)
	}

	stale, recompute := s.SimulationStatus(loaded)
	if stale || len(recompute) != 0 {
		t.Fatalf("unchanged ledger must keep every conclusion: stale=%v %v", stale, recompute)
	}

	// The real ledger moves: the holder is confirmed finished.
	s.ConfirmFinished(batchID, "holder")
	stale, recompute = s.SimulationStatus(loaded)
	if !stale {
		t.Fatal("ledger change must mark the saved simulation stale")
	}
	joined := strings.Join(recompute, "\n")
	if !strings.Contains(joined, "comboA") || !strings.Contains(joined, "d1") {
		t.Fatalf("comboA's conclusion depends on d1 and must be recomputed: %v", recompute)
	}
	if !strings.Contains(joined, "comboB") {
		t.Fatalf("comboB's blocked verdict depended on comboA and must be recomputed: %v", recompute)
	}

	// A fresh simulation on the new ledger agrees with reality: comboA was
	// really granted its combination, and comboB now waits on comboA's hold.
	waitFor(t, "comboA to acquire after confirmation", func() bool {
		for _, r := range s.Reservations() {
			if r.CaseID == "comboA" {
				return true
			}
		}
		return false
	})
	fresh := s.Simulate(SimulationRequest{})
	if len(fresh.Proceed) != 0 {
		t.Fatalf("comboB must wait for comboA's real hold: %+v", fresh.Proceed)
	}
	b2 := blockedVerdict(t, fresh, "comboB")
	if len(b2.BlockedByHolds) != 1 || b2.BlockedByHolds[0].CaseID != "comboA" {
		t.Fatalf("comboB should be blocked by comboA's d2 hold: %+v", b2.BlockedByHolds)
	}
}
