package testsched

import (
	"context"
	"errors"
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
	for {
		cur, ok := m.waiterLocked(req.seq)
		if !ok {
			m.broadcastLocked()
			m.mu.Unlock()
			return nil, ErrWaitCanceled
		}
		if cur.canceled {
			m.removeWaiterLocked(req.seq)
			m.broadcastLocked()
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
			m.version++
			m.mu.Unlock()
			return func() { m.release(res.owner(), granted) }, nil
		}
		if ctx.Err() != nil {
			m.removeWaiterLocked(req.seq)
			m.version++
			m.broadcastLocked()
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
		m.version++
		m.broadcastLocked()
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
		m.version++
		m.broadcastLocked()
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
		if r.owner() == owner && !r.Uncertain {
			r.Uncertain = true
			m.holds[name] = r
			changed = true
		}
	}
	if changed {
		m.version++
	}
}

// ledgerVersion returns the current ledger version. Every mutation of holds
// or waiters (grant, release, force release, uncertain marking, shrink,
// cancel, import) increments it, so a saved version identifies exactly the
// ledger state a read-only observation was derived from.
func (m *resourceManager) ledgerVersion() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.version
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

func (m *resourceManager) removeWaiterLocked(seq uint64) {
	for i, w := range m.waiters {
		if w.seq == seq {
			m.waiters = append(m.waiters[:i], m.waiters[i+1:]...)
			return
		}
	}
}

func (m *resourceManager) broadcastLocked() {
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
	out := make([]Waiter, 0, len(m.waiters))
	for _, w := range m.waiters {
		if w.canceled {
			continue
		}
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
		out = append(out, wt)
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
		m.version++
		m.broadcastLocked()
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
		m.version++
		m.broadcastLocked()
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
	}
	if len(m.holds) > 0 {
		m.version++
		m.broadcastLocked()
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
