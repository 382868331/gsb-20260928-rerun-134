package testsched

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ErrWaitCanceled is returned by acquire when the pending request was
// canceled via CancelWait before it could be granted.
var ErrWaitCanceled = errors.New("resource wait canceled")

// Reservation records one shared resource currently held by a case execution.
// Holds live on the scheduler, not on a single batch: competing batches see
// each other's reservations, and a lost worker keeps its hold until its run
// actually returns or a human confirms completion via ConfirmFinished.
type Reservation struct {
	Resource  string
	BatchID   string
	CaseID    string
	WorkerID  int
	Uncertain bool
	Since     time.Time
}

func (r Reservation) owner() string { return r.BatchID + "/" + r.CaseID }

// resourceManager arbitrates scarce shared resources across every batch of
// one scheduler. Acquisition for one case is atomic (all of its resources or
// none, so no lock ordering is needed) and fair: a later request may not
// overtake an earlier request that needs any of the same resources.
type resourceManager struct {
	mu      sync.Mutex
	changed chan struct{}
	seq     uint64
	version uint64
	holds   map[string]Reservation
	waiters []resourceRequest
}

type resourceRequest struct {
	seq       uint64
	batchID   string
	caseID    string
	workerID  int
	resources []string
	since     time.Time
	canceled  bool
}

// Waiter explains why one pending resource request is still waiting: which
// resources of the combination are currently unavailable (Missing), who
// holds them (BlockedBy, including whether the holder is a lost worker whose
// hold is uncertain), and the full combination originally requested.
type Waiter struct {
	BatchID   string
	CaseID    string
	WorkerID  int
	Resources []string
	Missing   []string
	BlockedBy []Reservation
	Since     time.Time
}

// ResourceOwner identifies one execution currently owning reservations.
type ResourceOwner struct {
	BatchID string `json:"batch_id"`
	CaseID  string `json:"case_id"`
}

// WaiterShrink is a hypothetical request to narrow a waiting combination.
type WaiterShrink struct {
	BatchID string   `json:"batch_id"`
	CaseID  string   `json:"case_id"`
	Keep    []string `json:"keep"`
}

// WaitingWhatIf describes a read-only resource-queue simulation. Finished
// owners are treated as if all their holds had ended, while Shrinks are
// treated as if the matching waiting requests had been narrowed.
type WaitingWhatIf struct {
	Finished []ResourceOwner `json:"finished_owners,omitempty"`
	Shrinks  []WaiterShrink  `json:"shrinks,omitempty"`
}

// SimulatedWaiter is the result of evaluating one request against copied
// ledger state. EarlierBlockers lists earlier intersecting combinations that
// preserve arrival-order fairness even when those combinations are also
// blocked by other resources.
type SimulatedWaiter struct {
	Waiter
	EarlierBlockers []Waiter `json:"earlier_blockers,omitempty"`
}

// FinishedReservation describes one occupation removed by a simulation.
type FinishedReservation struct {
	Owner        ResourceOwner `json:"owner"`
	Reservations []Reservation `json:"reservations"`
}

// ShrunkWaiter describes a request combination replaced during simulation.
type ShrunkWaiter struct {
	WaiterShrink
	From []string `json:"from"`
}

// WaitingSimulation is a persisted what-if snapshot. LedgerVersion is the
// ledger/queue version at computation time; conclusions from an older
// simulation must be recomputed once the live version differs.
type WaitingSimulation struct {
	LedgerVersion uint64                `json:"ledger_version"`
	GeneratedAt   time.Time             `json:"generated_at"`
	WhatIf        WaitingWhatIf         `json:"what_if"`
	BaselineHolds []Reservation         `json:"baseline_holds"`
	BaselineWait  []Waiter              `json:"baseline_waiters"`
	Released      []FinishedReservation `json:"released_holds,omitempty"`
	Shrunk        []ShrunkWaiter        `json:"shrunk_waiters,omitempty"`
	Ready         []SimulatedWaiter     `json:"ready"`
	Blocked       []SimulatedWaiter     `json:"blocked"`
}

// SimulationConclusion refers to one ready/blocked conclusion in a saved
// simulation that should be recomputed after ledger changes.
type SimulationConclusion struct {
	BatchID string `json:"batch_id"`
	CaseID  string `json:"case_id"`
}

// SimulationFreshness compares a saved simulation with the live ledger. When
// Current is false, ChangedResources and StaleConclusions explain which
// resources and saved conclusions are affected. A version mismatch with no
// more specific dependency still makes the whole simulation stale because an
// arrival-order or queue-membership change may have occurred.
type SimulationFreshness struct {
	Current          bool                   `json:"current"`
	SavedVersion     uint64                 `json:"saved_version"`
	CurrentVersion   uint64                 `json:"current_version"`
	ChangedResources []string               `json:"changed_resources,omitempty"`
	StaleConclusions []SimulationConclusion `json:"stale_conclusions,omitempty"`
}

func newResourceManager() *resourceManager {
	return &resourceManager{
		changed: make(chan struct{}),
		holds:   map[string]Reservation{},
	}
}

// acquire blocks until all named resources are free and grants them to res.
// The returned release only drops holds still owned by res, so a hold that
// was force-released and re-granted to someone else is never stolen back.
func (m *resourceManager) acquire(ctx context.Context, res Reservation, resources []string) (release func(), err error) {
	if len(resources) == 0 {
		return func() {}, nil
	}
	m.mu.Lock()
	m.seq++
	req := resourceRequest{
		seq:       m.seq,
		batchID:   res.BatchID,
		caseID:    res.CaseID,
		workerID:  res.WorkerID,
		resources: resources,
		since:     time.Now(),
	}
	m.waiters = append(m.waiters, req)
	m.touchLocked()
	for {
		cur, ok := m.waiterLocked(req.seq)
		if !ok {
			m.touchLocked()
			m.mu.Unlock()
			return nil, ErrWaitCanceled
		}
		if cur.canceled {
			m.removeWaiterLocked(req.seq)
			m.touchLocked()
			m.mu.Unlock()
			return nil, ErrWaitCanceled
		}
		if m.canGrantLocked(cur) {
			granted := append([]string(nil), cur.resources...)
			for _, name := range granted {
				r := res
				r.Resource = name
				r.Since = time.Now()
				m.holds[name] = r
			}
			m.removeWaiterLocked(req.seq)
			m.touchLocked()
			m.mu.Unlock()
			return func() { m.release(res.owner(), granted) }, nil
		}
		if ctx.Err() != nil {
			m.removeWaiterLocked(req.seq)
			m.touchLocked()
			m.mu.Unlock()
			return nil, ctx.Err()
		}
		changed := m.changed
		m.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
		m.mu.Lock()
	}
}

// waiterLocked returns the current state of a pending request, so a blocked
// acquire observes shrinks and cancels applied while it was waiting.
func (m *resourceManager) waiterLocked(seq uint64) (resourceRequest, bool) {
	for _, w := range m.waiters {
		if w.seq == seq {
			return w, true
		}
	}
	return resourceRequest{}, false
}

func (m *resourceManager) canGrantLocked(req resourceRequest) bool {
	for _, name := range req.resources {
		if _, ok := m.holds[name]; ok {
			return false
		}
	}
	// Fairness: an earlier waiter competing for any of the same resources
	// must be served first. Waiters are kept in arrival (seq) order.
	for _, w := range m.waiters {
		if w.seq >= req.seq {
			break
		}
		if sharesAny(w.resources, req.resources) {
			return false
		}
	}
	return true
}

func (m *resourceManager) release(owner string, resources []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	changed := false
	for _, name := range resources {
		if r, ok := m.holds[name]; ok && r.owner() == owner {
			delete(m.holds, name)
			changed = true
		}
	}
	if changed {
		m.touchLocked()
	}
}

// forceRelease drops every hold owned by the given batch/case, regardless of
// whether its run goroutine is still blocked. It returns the dropped
// reservations so the caller keeps evidence of what was freed.
func (m *resourceManager) forceRelease(batchID, caseID string) []Reservation {
	m.mu.Lock()
	defer m.mu.Unlock()
	owner := batchID + "/" + caseID
	var released []Reservation
	for name, r := range m.holds {
		if r.owner() == owner {
			released = append(released, r)
			delete(m.holds, name)
		}
	}
	if len(released) > 0 {
		sort.Slice(released, func(i, j int) bool { return released[i].Resource < released[j].Resource })
		m.touchLocked()
	}
	return released
}

// markUncertain flags holds whose worker was lost while the case may still be
// executing, so observers can see the resource is not safely reusable yet.
func (m *resourceManager) markUncertain(batchID, caseID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	owner := batchID + "/" + caseID
	changed := false
	for name, r := range m.holds {
		if r.owner() == owner {
			r.Uncertain = true
			m.holds[name] = r
			changed = true
		}
	}
	if changed {
		m.touchLocked()
	}
}

func (m *resourceManager) snapshot() []Reservation {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Reservation, 0, len(m.holds))
	for _, r := range m.holds {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Resource < out[j].Resource })
	return out
}

type ledgerSnapshot struct {
	version uint64
	holds   []Reservation
	waiters []resourceRequest
}

func (m *resourceManager) ledgerSnapshot() ledgerSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	holds := make([]Reservation, 0, len(m.holds))
	for _, r := range m.holds {
		holds = append(holds, r)
	}
	sort.Slice(holds, func(i, j int) bool { return holds[i].Resource < holds[j].Resource })
	waiters := append([]resourceRequest(nil), m.waiters...)
	return ledgerSnapshot{version: m.version, holds: holds, waiters: waiters}
}

func (m *resourceManager) currentVersion() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.version
}

func (m *resourceManager) removeWaiterLocked(seq uint64) {
	for i, w := range m.waiters {
		if w.seq == seq {
			m.waiters = append(m.waiters[:i], m.waiters[i+1:]...)
			return
		}
	}
}

func (m *resourceManager) touchLocked() {
	m.version++
	close(m.changed)
	m.changed = make(chan struct{})
}

func sharesAny(a, b []string) bool {
	for _, x := range a {
		for _, y := range b {
			if x == y {
				return true
			}
		}
	}
	return false
}

// waiting snapshots every pending request together with an explanation:
// which resources of the combination are still missing and which
// reservations currently block them. Waiters never hold a partial
// combination, so a waiting request owns nothing yet.
func (m *resourceManager) waiting() []Waiter {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.waitingLocked()
}

func waiterFromRequestLocked(m *resourceManager, w resourceRequest) Waiter {
	wt := Waiter{
		BatchID:   w.batchID,
		CaseID:    w.caseID,
		WorkerID:  w.workerID,
		Resources: append([]string(nil), w.resources...),
		Since:     w.since,
	}
	seen := map[string]bool{}
	for _, name := range w.resources {
		if r, ok := m.holds[name]; ok {
			wt.Missing = append(wt.Missing, name)
			if !seen[r.owner()] {
				seen[r.owner()] = true
				wt.BlockedBy = append(wt.BlockedBy, r)
			}
		}
	}
	sort.Strings(wt.Missing)
	sort.Slice(wt.BlockedBy, func(i, j int) bool { return wt.BlockedBy[i].Resource < wt.BlockedBy[j].Resource })
	return wt
}

func (m *resourceManager) waitingLocked() []Waiter {
	out := make([]Waiter, 0, len(m.waiters))
	for _, w := range m.waiters {
		if w.canceled {
			continue
		}
		out = append(out, waiterFromRequestLocked(m, w))
	}
	return out
}

// shrink reduces a pending request to the given subset of its resources, so
// a case that no longer needs the full combination can proceed with less.
// It only affects requests still waiting; holds already granted to a
// running execution are untouched and keep reflecting the real state.
// Returns false when no matching waiter exists or keep is not a non-empty
// subset of the requested resources.
func (m *resourceManager) shrink(batchID, caseID string, keep []string) bool {
	if len(keep) == 0 {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	changed := false
	for i, w := range m.waiters {
		if w.batchID != batchID || w.caseID != caseID || w.canceled {
			continue
		}
		if !isSubset(keep, w.resources) {
			return false
		}
		m.waiters[i].resources = append([]string(nil), keep...)
		changed = true
	}
	if changed {
		m.touchLocked()
	}
	return changed
}

// cancelWait drops every pending request of the given batch/case. The
// blocked acquire returns ErrWaitCanceled and the case is recorded as
// not_run. Holds already granted to its executions are not affected.
// Returns false when nothing was waiting.
func (m *resourceManager) cancelWait(batchID, caseID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	changed := false
	for i, w := range m.waiters {
		if w.batchID == batchID && w.caseID == caseID && !w.canceled {
			m.waiters[i].canceled = true
			changed = true
		}
	}
	if changed {
		m.touchLocked()
	}
	return changed
}

// importHolds restores previously saved reservations, e.g. after a restart.
// A resource already held by someone else is never reassigned: conflicting
// entries are skipped and returned so the caller can reconcile. Restored
// holds keep their original owner and Uncertain flag, so a lost worker's
// occupation survives the save/load round trip until it is confirmed.
func (m *resourceManager) importHolds(rs []Reservation) []Reservation {
	m.mu.Lock()
	defer m.mu.Unlock()
	var conflicts []Reservation
	imported := false
	for _, r := range rs {
		if r.Resource == "" {
			continue
		}
		if existing, ok := m.holds[r.Resource]; ok {
			if existing.owner() != r.owner() {
				conflicts = append(conflicts, r)
			}
			continue
		}
		m.holds[r.Resource] = r
		imported = true
	}
	if imported {
		m.touchLocked()
	}
	sort.Slice(conflicts, func(i, j int) bool { return conflicts[i].Resource < conflicts[j].Resource })
	return conflicts
}

func isSubset(keep, resources []string) bool {
	for _, k := range keep {
		found := false
		for _, r := range resources {
			if k == r {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// simulateWaiting evaluates a hypothetical release/shrink scenario on copies
// of the current holds and queue. It never grants a simulated request:
// later waiters are still checked in arrival order, so an earlier blocked
// cross-combination remains visible as a fairness barrier.
func (m *resourceManager) simulateWaiting(what WaitingWhatIf, now time.Time) (WaitingSimulation, error) {
	snap := m.ledgerSnapshot()
	sim, err := buildWaitingSimulation(snap, what, now)
	if err != nil {
		return WaitingSimulation{}, err
	}
	return sim, nil
}

func buildWaitingSimulation(snap ledgerSnapshot, what WaitingWhatIf, now time.Time) (WaitingSimulation, error) {
	holds := map[string]Reservation{}
	for _, r := range snap.holds {
		holds[r.Resource] = r
	}
	waiters := make([]resourceRequest, len(snap.waiters))
	copy(waiters, snap.waiters)

	result := WaitingSimulation{
		LedgerVersion: snap.version,
		GeneratedAt:   now,
		BaselineHolds: append([]Reservation(nil), snap.holds...),
		BaselineWait:  waitersToWaiters(snap.waiters, holds),
	}

	finishedOwners := dedupeOwners(what.Finished)
	result.WhatIf.Finished = finishedOwners
	released := map[string]*FinishedReservation{}
	for name, r := range holds {
		for _, owner := range finishedOwners {
			if r.BatchID == owner.BatchID && r.CaseID == owner.CaseID {
				key := owner.BatchID + "/" + owner.CaseID
				if released[key] == nil {
					released[key] = &FinishedReservation{Owner: owner}
				}
				released[key].Reservations = append(released[key].Reservations, r)
				delete(holds, name)
				break
			}
		}
	}
	for _, owner := range finishedOwners {
		key := owner.BatchID + "/" + owner.CaseID
		if item := released[key]; item != nil {
			sort.Slice(item.Reservations, func(i, j int) bool { return item.Reservations[i].Resource < item.Reservations[j].Resource })
			result.Released = append(result.Released, *item)
		}
	}
	sort.Slice(result.Released, func(i, j int) bool {
		if result.Released[i].Owner.BatchID != result.Released[j].Owner.BatchID {
			return result.Released[i].Owner.BatchID < result.Released[j].Owner.BatchID
		}
		return result.Released[i].Owner.CaseID < result.Released[j].Owner.CaseID
	})

	shrinks := dedupeShrinks(what.Shrinks)
	result.WhatIf.Shrinks = shrinks
	for _, shrink := range shrinks {
		keep := dedupeSortedResources(shrink.Keep)
		if len(keep) == 0 {
			return WaitingSimulation{}, fmt.Errorf("simulated shrink for %s/%s must keep a non-empty subset", shrink.BatchID, shrink.CaseID)
		}
		matched := false
		for i, w := range waiters {
			if w.canceled || w.batchID != shrink.BatchID || w.caseID != shrink.CaseID {
				continue
			}
			matched = true
			if !isSubset(keep, w.resources) {
				return WaitingSimulation{}, fmt.Errorf("simulated shrink for %s/%s keeps resources outside its current combination", shrink.BatchID, shrink.CaseID)
			}
			result.Shrunk = append(result.Shrunk, ShrunkWaiter{
				WaiterShrink: WaiterShrink{
					BatchID: shrink.BatchID,
					CaseID:  shrink.CaseID,
					Keep:    append([]string(nil), keep...),
				},
				From: append([]string(nil), w.resources...),
			})
			waiters[i].resources = append([]string(nil), keep...)
		}
		if !matched {
			return WaitingSimulation{}, fmt.Errorf("simulated shrink target %s/%s is not waiting", shrink.BatchID, shrink.CaseID)
		}
	}
	sort.Slice(result.Shrunk, func(i, j int) bool {
		if result.Shrunk[i].BatchID != result.Shrunk[j].BatchID {
			return result.Shrunk[i].BatchID < result.Shrunk[j].BatchID
		}
		if result.Shrunk[i].CaseID != result.Shrunk[j].CaseID {
			return result.Shrunk[i].CaseID < result.Shrunk[j].CaseID
		}
		return stringsJoin(result.Shrunk[i].Keep) < stringsJoin(result.Shrunk[j].Keep)
	})

	for i, w := range waiters {
		if w.canceled {
			continue
		}
		item := SimulatedWaiter{Waiter: waiterFromResources(w, holds)}
		for j := 0; j < i; j++ {
			earlier := waiters[j]
			if earlier.canceled {
				continue
			}
			if sharesAny(earlier.resources, w.resources) {
				item.EarlierBlockers = append(item.EarlierBlockers, waiterFromResources(earlier, holds))
			}
		}
		if len(item.Missing) == 0 && len(item.EarlierBlockers) == 0 {
			result.Ready = append(result.Ready, item)
		} else {
			result.Blocked = append(result.Blocked, item)
		}
	}
	return result, nil
}

func waiterFromResources(w resourceRequest, holds map[string]Reservation) Waiter {
	wt := Waiter{
		BatchID:   w.batchID,
		CaseID:    w.caseID,
		WorkerID:  w.workerID,
		Resources: append([]string(nil), w.resources...),
		Since:     w.since,
	}
	seen := map[string]bool{}
	for _, name := range w.resources {
		if r, ok := holds[name]; ok {
			wt.Missing = append(wt.Missing, name)
			owner := r.BatchID + "/" + r.CaseID
			if !seen[owner] {
				seen[owner] = true
				wt.BlockedBy = append(wt.BlockedBy, r)
			}
		}
	}
	sort.Strings(wt.Missing)
	sort.Slice(wt.BlockedBy, func(i, j int) bool { return wt.BlockedBy[i].Resource < wt.BlockedBy[j].Resource })
	return wt
}

func waitersToWaiters(ws []resourceRequest, holds map[string]Reservation) []Waiter {
	out := make([]Waiter, 0, len(ws))
	for _, w := range ws {
		if !w.canceled {
			out = append(out, waiterFromResources(w, holds))
		}
	}
	return out
}

func dedupeOwners(owners []ResourceOwner) []ResourceOwner {
	seen := map[string]bool{}
	var out []ResourceOwner
	for _, owner := range owners {
		if owner.BatchID == "" || owner.CaseID == "" {
			continue
		}
		key := owner.BatchID + "/" + owner.CaseID
		if !seen[key] {
			seen[key] = true
			out = append(out, owner)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].BatchID != out[j].BatchID {
			return out[i].BatchID < out[j].BatchID
		}
		return out[i].CaseID < out[j].CaseID
	})
	return out
}

func dedupeShrinks(shrinks []WaiterShrink) []WaiterShrink {
	seen := map[string]bool{}
	var out []WaiterShrink
	for _, shrink := range shrinks {
		if shrink.BatchID == "" || shrink.CaseID == "" {
			continue
		}
		key := shrink.BatchID + "/" + shrink.CaseID
		if !seen[key] {
			seen[key] = true
			out = append(out, WaiterShrink{
				BatchID: shrink.BatchID,
				CaseID:  shrink.CaseID,
				Keep:    dedupeSortedResources(shrink.Keep),
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].BatchID != out[j].BatchID {
			return out[i].BatchID < out[j].BatchID
		}
		return out[i].CaseID < out[j].CaseID
	})
	return out
}

func dedupeSortedResources(resources []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, name := range resources {
		if name != "" && !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func stringsJoin(values []string) string {
	out := ""
	for i, value := range values {
		if i > 0 {
			out += ","
		}
		out += value
	}
	return out
}

func (m *resourceManager) simulationFreshness(saved WaitingSimulation) SimulationFreshness {
	snap := m.ledgerSnapshot()
	fresh := SimulationFreshness{
		SavedVersion:   saved.LedgerVersion,
		CurrentVersion: snap.version,
		Current:        snap.version == saved.LedgerVersion,
	}
	if fresh.Current {
		return fresh
	}

	changed := changedHoldResources(saved.BaselineHolds, snap.holds)
	var currentWaiters []resourceRequest
	for _, w := range snap.waiters {
		if !w.canceled {
			currentWaiters = append(currentWaiters, w)
		}
	}
	if len(currentWaiters) != len(saved.BaselineWait) {
		changed["*"] = true
	} else {
		for i, current := range currentWaiters {
			old := saved.BaselineWait[i]
			if current.batchID != old.BatchID || current.caseID != old.CaseID {
				changed["*"] = true
				break
			}
			if !sameStringSet(old.Resources, current.resources) {
				for _, name := range old.Resources {
					changed[name] = true
				}
				for _, name := range current.resources {
					changed[name] = true
				}
			}
		}
	}

	for _, groups := range [][]SimulatedWaiter{saved.Ready, saved.Blocked} {
		for _, w := range groups {
			dirty := changed["*"]
			for _, name := range w.Resources {
				if changed[name] {
					dirty = true
				}
			}
			if dirty {
				fresh.StaleConclusions = append(fresh.StaleConclusions, SimulationConclusion{BatchID: w.BatchID, CaseID: w.CaseID})
			}
		}
	}
	for name := range changed {
		if name != "*" {
			fresh.ChangedResources = append(fresh.ChangedResources, name)
		}
	}
	sort.Strings(fresh.ChangedResources)
	sort.Slice(fresh.StaleConclusions, func(i, j int) bool {
		if fresh.StaleConclusions[i].BatchID != fresh.StaleConclusions[j].BatchID {
			return fresh.StaleConclusions[i].BatchID < fresh.StaleConclusions[j].BatchID
		}
		return fresh.StaleConclusions[i].CaseID < fresh.StaleConclusions[j].CaseID
	})
	return fresh
}

func changedHoldResources(before []Reservation, after []Reservation) map[string]bool {
	current := map[string]Reservation{}
	for _, r := range after {
		current[r.Resource] = r
	}
	changed := map[string]bool{}
	for _, old := range before {
		new, ok := current[old.Resource]
		if !ok || new.owner() != old.owner() || new.Uncertain != old.Uncertain {
			changed[old.Resource] = true
		}
	}
	baseline := map[string]bool{}
	for _, r := range before {
		baseline[r.Resource] = true
	}
	for _, r := range after {
		if !baseline[r.Resource] {
			changed[r.Resource] = true
		}
	}
	return changed
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[string]int{}
	for _, value := range a {
		seen[value]++
	}
	for _, value := range b {
		seen[value]--
		if seen[value] < 0 {
			return false
		}
	}
	return true
}
