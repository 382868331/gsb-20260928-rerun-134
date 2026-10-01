package testsched

import (
	"context"
	"sort"
	"sync"
	"time"
)

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
	holds   map[string]Reservation
	waiters []resourceRequest
}

type resourceRequest struct {
	seq       uint64
	resources []string
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
	req := resourceRequest{seq: m.seq, resources: resources}
	m.waiters = append(m.waiters, req)
	for {
		if m.canGrantLocked(req) {
			for _, name := range resources {
				r := res
				r.Resource = name
				r.Since = time.Now()
				m.holds[name] = r
			}
			m.removeWaiterLocked(req.seq)
			m.mu.Unlock()
			return func() { m.release(res.owner(), resources) }, nil
		}
		if ctx.Err() != nil {
			m.removeWaiterLocked(req.seq)
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
	for _, name := range resources {
		if r, ok := m.holds[name]; ok && r.owner() == owner {
			delete(m.holds, name)
		}
	}
	m.broadcastLocked()
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
	for name, r := range m.holds {
		if r.owner() == owner {
			r.Uncertain = true
			m.holds[name] = r
		}
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
