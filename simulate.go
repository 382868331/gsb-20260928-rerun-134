package testsched

import (
	"sort"
	"strconv"
)

// Owner identifies one case execution by batch and case ID.
type Owner struct {
	BatchID string
	CaseID  string
}

func (o Owner) key() string { return o.BatchID + "/" + o.CaseID }

// SimulatedShrink describes one hypothetical narrowing of a pending
// request: the waiter for batch/case is assumed to keep only the listed
// subset of its combination. It mirrors ShrinkWait but is never applied to
// the real ledger.
type SimulatedShrink struct {
	BatchID string
	CaseID  string
	Keep    []string
}

// SimulationRequest asks what the wait queue would look like if the given
// holds had ended and the given waiters had shrunk. Nothing is acquired,
// released, or removed: the simulation runs on a private copy of the
// ledger.
type SimulationRequest struct {
	// ReleaseOwners lists owners whose current holds are assumed to have
	// ended (as if ConfirmFinished had been called for them).
	ReleaseOwners []Owner
	// Shrinks lists hypothetical waiter reductions.
	Shrinks []SimulatedShrink
}

// SimulatedWaiter is one waiting combination as it would look after the
// hypothetical changes, together with the verdict of the simulation.
type SimulatedWaiter struct {
	BatchID   string
	CaseID    string
	WorkerID  int
	Resources []string
	// BlockedByHolds lists reservations that would still occupy resources
	// of the combination after the hypothetical releases.
	BlockedByHolds []Reservation
	// BlockedByWaiters lists earlier ("batch/case") waiters whose
	// intersecting combinations would still be served first.
	BlockedByWaiters []string
}

func (w SimulatedWaiter) owner() string { return w.BatchID + "/" + w.CaseID }

// Simulation is the read-only outcome of one what-if run. Version records
// the ledger version the conclusions were derived from; once the real
// ledger moves past it, SimulationStatus reports which conclusions must be
// recomputed. ObservedHolds and ObservedWaiters capture the ledger state at
// simulation time so a later comparison can name exactly what changed.
type Simulation struct {
	Version        uint64
	ReleasedOwners []string
	Shrinks        []SimulatedShrink
	Proceed        []SimulatedWaiter
	Blocked        []SimulatedWaiter
	ObservedHolds  []Reservation
	// ObservedWaiters maps each pending "batch/case" owner to the
	// combination it was waiting for at simulation time.
	ObservedWaiters map[string][]string
}

// simulate runs the what-if scenario on a copy of the ledger. The real
// holds and waiters are never touched.
func (m *resourceManager) simulate(req SimulationRequest) Simulation {
	m.mu.Lock()
	defer m.mu.Unlock()

	sim := Simulation{Version: m.version}

	holds := make(map[string]Reservation, len(m.holds))
	for name, r := range m.holds {
		holds[name] = r
		sim.ObservedHolds = append(sim.ObservedHolds, r)
	}
	sort.Slice(sim.ObservedHolds, func(i, j int) bool { return sim.ObservedHolds[i].Resource < sim.ObservedHolds[j].Resource })

	type simWaiter struct {
		batchID   string
		caseID    string
		workerID  int
		resources []string
	}
	var waiters []simWaiter
	sim.ObservedWaiters = map[string][]string{}
	for _, w := range m.waiters {
		if w.canceled {
			continue
		}
		resources := append([]string(nil), w.resources...)
		waiters = append(waiters, simWaiter{
			batchID:   w.batchID,
			caseID:    w.caseID,
			workerID:  w.workerID,
			resources: resources,
		})
		sim.ObservedWaiters[w.batchID+"/"+w.caseID] = resources
	}

	// Hypothetical releases: drop holds of the named owners from the copy.
	released := map[string]bool{}
	for _, o := range req.ReleaseOwners {
		key := o.key()
		if released[key] {
			continue
		}
		released[key] = true
		sim.ReleasedOwners = append(sim.ReleasedOwners, key)
		for name, r := range holds {
			if r.owner() == key {
				delete(holds, name)
			}
		}
	}
	sort.Strings(sim.ReleasedOwners)

	// Hypothetical shrinks: narrow matching waiters in the copy. A shrink
	// that matches no waiter or is not a subset of its combination is kept
	// in the report so the caller can see it had no effect.
	for _, sh := range req.Shrinks {
		entry := SimulatedShrink{BatchID: sh.BatchID, CaseID: sh.CaseID, Keep: append([]string(nil), sh.Keep...)}
		sim.Shrinks = append(sim.Shrinks, entry)
		for i, w := range waiters {
			if w.batchID == entry.BatchID && w.caseID == entry.CaseID &&
				len(entry.Keep) > 0 && isSubset(entry.Keep, w.resources) {
				waiters[i].resources = append([]string(nil), entry.Keep...)
			}
		}
	}

	// Replay the fairness rule in arrival order: a combination proceeds
	// when every resource is free in the copied ledger and no earlier
	// intersecting combination is still waiting.
	proceeded := make([]bool, len(waiters))
	for i, w := range waiters {
		sw := SimulatedWaiter{
			BatchID:   w.batchID,
			CaseID:    w.caseID,
			WorkerID:  w.workerID,
			Resources: append([]string(nil), w.resources...),
		}
		seenHold := map[string]bool{}
		seenWaiter := map[string]bool{}
		for _, name := range w.resources {
			if r, ok := holds[name]; ok {
				if !seenHold[r.owner()] {
					seenHold[r.owner()] = true
					sw.BlockedByHolds = append(sw.BlockedByHolds, r)
				}
			}
		}
		for j := 0; j < i; j++ {
			if proceeded[j] {
				continue
			}
			key := waiters[j].batchID + "/" + waiters[j].caseID
			if sharesAny(waiters[j].resources, w.resources) && !seenWaiter[key] {
				seenWaiter[key] = true
				sw.BlockedByWaiters = append(sw.BlockedByWaiters, key)
			}
		}
		if len(sw.BlockedByHolds) == 0 && len(sw.BlockedByWaiters) == 0 {
			proceeded[i] = true
			for _, name := range w.resources {
				holds[name] = Reservation{Resource: name, BatchID: w.batchID, CaseID: w.caseID, WorkerID: w.workerID}
			}
			sim.Proceed = append(sim.Proceed, sw)
			continue
		}
		sort.Slice(sw.BlockedByHolds, func(a, b int) bool { return sw.BlockedByHolds[a].Resource < sw.BlockedByHolds[b].Resource })
		sort.Strings(sw.BlockedByWaiters)
		sim.Blocked = append(sim.Blocked, sw)
	}
	return sim
}

// simulationStatus compares a saved simulation with the current ledger and
// reports which of its conclusions are stale. When the ledger version is
// unchanged every conclusion still stands; otherwise each returned entry
// names one conclusion that must be recomputed and why. Conclusions not
// listed are unaffected by the observed changes.
func (m *resourceManager) simulationStatus(sim Simulation) (stale bool, recompute []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.version == sim.Version {
		return false, nil
	}
	stale = true
	version := strconv.FormatUint(sim.Version, 10)

	changedResources := map[string]bool{}
	observedHolds := map[string]Reservation{}
	for _, r := range sim.ObservedHolds {
		observedHolds[r.Resource] = r
	}
	for name, r := range observedHolds {
		cur, ok := m.holds[name]
		if !ok || cur.owner() != r.owner() || cur.Uncertain != r.Uncertain {
			changedResources[name] = true
		}
	}
	for name := range m.holds {
		if _, ok := observedHolds[name]; !ok {
			changedResources[name] = true
		}
	}

	currentWaiters := map[string][]string{}
	for _, w := range m.waiters {
		if w.canceled {
			continue
		}
		currentWaiters[w.batchID+"/"+w.caseID] = w.resources
	}
	changedWaiters := map[string]bool{}
	for key := range sim.ObservedWaiters {
		if _, ok := currentWaiters[key]; !ok {
			changedWaiters[key] = true
		}
	}
	for key := range currentWaiters {
		if _, ok := sim.ObservedWaiters[key]; !ok {
			changedWaiters[key] = true
		}
	}

	check := func(kind string, w SimulatedWaiter) {
		key := w.owner()
		if _, ok := currentWaiters[key]; !ok {
			recompute = append(recompute, kind+" conclusion for "+key+": waiter no longer pending (ledger was version "+version+")")
			return
		}
		for _, name := range w.Resources {
			if changedResources[name] {
				recompute = append(recompute, kind+" conclusion for "+key+": ledger for resource "+name+" changed since version "+version)
				return
			}
		}
		for other := range sim.ObservedWaiters {
			if other == key {
				continue
			}
			otherResources, ok := currentWaiters[other]
			if !ok {
				otherResources = sim.ObservedWaiters[other]
			}
			if !sharesAny(otherResources, w.Resources) {
				continue
			}
			affected := changedWaiters[other]
			if !affected {
				for _, name := range otherResources {
					if changedResources[name] {
						affected = true
						break
					}
				}
			}
			if affected {
				recompute = append(recompute, kind+" conclusion for "+key+": intersecting waiter "+other+" changed since version "+version)
				return
			}
		}
	}
	for _, w := range sim.Proceed {
		check("proceed", w)
	}
	for _, w := range sim.Blocked {
		check("blocked", w)
	}
	sort.Strings(recompute)
	return stale, recompute
}
