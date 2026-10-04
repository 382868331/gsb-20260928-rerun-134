package testsched

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ErrRequirementCanceled is returned to a waiting acquisition whose resource
// requirement was canceled before the full combination became available.
var ErrRequirementCanceled = errors.New("resource requirement canceled")

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
	owner     string
	resources []string
	canceled  bool
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
	req := resourceRequest{seq: m.seq, owner: res.owner(), resources: resources}
	m.waiters = append(m.waiters, req)
	for {
		// Re-read the live request each round: it may have been narrowed
		// or canceled while this goroutine was asleep.
		cur := m.findWaiterLocked(req.seq)
		if cur.canceled {
			m.removeWaiterLocked(req.seq)
			m.broadcastLocked()
			m.mu.Unlock()
			return nil, ErrRequirementCanceled
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
			m.mu.Unlock()
			return func() { m.release(res.owner(), granted) }, nil
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

// cancelWaiter drops a still-waiting requirement owned by batch/case.
// Granted holds are untouched: devices already handed to a worker keep
// following the real run state. Returns true if a waiter was found.
func (m *resourceManager) cancelWaiter(owner string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.waiters {
		if m.waiters[i].owner == owner {
			m.waiters[i].canceled = true
			m.broadcastLocked()
			return true
		}
	}
	return false
}

// narrowWaiter shrinks a still-waiting requirement to a subset of its
// current resources. Widening and empty sets are rejected; granted holds
// are unaffected.
func (m *resourceManager) narrowWaiter(owner string, resources []string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.waiters {
		if m.waiters[i].owner != owner {
			continue
		}
		cur := map[string]bool{}
		for _, r := range m.waiters[i].resources {
			cur[r] = true
		}
		next := dedupeSorted(append([]string(nil), resources...))
		sort.Strings(next)
		if len(next) == 0 {
			return false
		}
		for _, r := range next {
			if !cur[r] {
				return false
			}
		}
		m.waiters[i].resources = next
		m.broadcastLocked()
		return true
	}
	return false
}

func (m *resourceManager) findWaiterLocked(seq uint64) resourceRequest {
	for _, w := range m.waiters {
		if w.seq == seq {
			return w
		}
	}
	return resourceRequest{seq: seq, canceled: true}
}

// WaitExplanation describes why one waiting requirement has not started:
// the capabilities it still lacks, who currently holds them, and which
// earlier competing waiters are ahead of it in the fair queue.
type WaitExplanation struct {
	BatchID  string
	CaseID   string
	Required []string
	Missing  []string
	// HeldBy maps each missing capability to the batch/case owner holding
	// it. Uncertain holds (lost workers) are included: they are not safely
	// reusable until confirmed.
	HeldBy map[string]string
	// Uncertain lists missing capabilities held by lost workers.
	Uncertain []string
	// AheadOf lists owners of earlier waiters competing for the same
	// capabilities, which fairness serves first.
	AheadOf []string
}

// waiting explains every currently waiting requirement.
func (m *resourceManager) waiting() []WaitExplanation {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]WaitExplanation, 0, len(m.waiters))
	for i, w := range m.waiters {
		ex := WaitExplanation{
			Required: append([]string(nil), w.resources...),
			HeldBy:   map[string]string{},
		}
		ex.BatchID, ex.CaseID = splitOwner(w.owner)
		for _, name := range w.resources {
			if r, ok := m.holds[name]; ok {
				ex.Missing = append(ex.Missing, name)
				ex.HeldBy[name] = r.owner()
				if r.Uncertain {
					ex.Uncertain = append(ex.Uncertain, name)
				}
			}
		}
		for _, earlier := range m.waiters[:i] {
			if sharesAny(earlier.resources, w.resources) {
				ex.AheadOf = append(ex.AheadOf, earlier.owner)
			}
		}
		out = append(out, ex)
	}
	return out
}

func splitOwner(owner string) (batchID, caseID string) {
	for i, c := range owner {
		if c == '/' {
			return owner[:i], owner[i+1:]
		}
	}
	return owner, ""
}

// importHolds restores previously saved holds, refusing any resource that
// is already held so a device can never belong to two executions at once.
func (m *resourceManager) importHolds(holds []Reservation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range holds {
		if r.Resource == "" {
			return fmt.Errorf("saved hold with empty resource")
		}
		if _, ok := m.holds[r.Resource]; ok {
			return fmt.Errorf("resource %s already held", r.Resource)
		}
	}
	for _, r := range holds {
		m.holds[r.Resource] = r
	}
	if len(holds) > 0 {
		m.broadcastLocked()
	}
	return nil
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
