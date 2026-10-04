package testsched

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	Attempt   int
}

func (r Reservation) owner() string { return r.BatchID + "/" + r.CaseID }

func (r Reservation) executionKey() string {
	return fmt.Sprintf("%s/%d/%d", r.owner(), r.Attempt, r.WorkerID)
}

// MissingResource explains why one device in a requested combination is not
// available yet. Holder is the current reservation blocking that device.
type MissingResource struct {
	Resource string
	Holder   Reservation
}

// WaitStatus explains a complete-resource request that is still waiting.
// Waiting requests never hold any part of their requested combination.
type WaitStatus struct {
	BatchID   string
	CaseID    string
	WorkerID  int
	Attempt   int
	Requested []string
	Missing   []MissingResource
}

// State is the persistent resource account. Only devices currently delivered
// to an execution are saved; transient waiters are not reservations.
type State struct {
	Reservations []Reservation `json:"reservations"`
}

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
	res       Reservation
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
	m.mu.Lock()
	resources = normalizeResources(resources)
	if len(resources) == 0 {
		m.mu.Unlock()
		return func() {}, nil
	}
	res.Attempt = attemptOrDefault(res.Attempt)
	m.seq++
	req := resourceRequest{seq: m.seq, res: res, resources: resources}
	m.waiters = append(m.waiters, req)
	for {
		current := m.requestLocked(req.seq)
		if current == nil {
			m.mu.Unlock()
			return nil, context.Canceled
		}
		req = *current
		if m.canGrantLocked(req) {
			granted := req.res
			for _, name := range req.resources {
				r := granted
				r.Resource = name
				r.Since = time.Now()
				m.holds[name] = r
			}
			m.removeWaiterLocked(req.seq)
			m.mu.Unlock()
			return func() { m.releaseExecution(granted) }, nil
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

func (m *resourceManager) requestLocked(seq uint64) *resourceRequest {
	for i := range m.waiters {
		if m.waiters[i].seq == seq {
			if m.waiters[i].canceled {
				m.waiters = append(m.waiters[:i], m.waiters[i+1:]...)
				m.broadcastLocked()
				return nil
			}
			return &m.waiters[i]
		}
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
		if w.canceled {
			continue
		}
		if w.seq >= req.seq {
			break
		}
		if sharesAny(w.resources, req.resources) {
			return false
		}
	}
	return true
}

func (m *resourceManager) releaseExecution(res Reservation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, r := range m.holds {
		if r.executionKey() != res.executionKey() {
			continue
		}
		delete(m.holds, name)
	}
	m.broadcastLocked()
}

// shrinkWaiting changes a request that has not yet been granted. The new
// combination must be a subset of the original request. Shrinking never
// releases a device already delivered to a running execution.
func (m *resourceManager) shrinkWaiting(batchID, caseID string, resources []string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	wanted := normalizeResources(resources)
	for i := range m.waiters {
		w := &m.waiters[i]
		if w.res.BatchID != batchID || w.res.CaseID != caseID || w.canceled {
			continue
		}
		if !isSubset(wanted, w.resources) {
			return false
		}
		w.resources = wanted
		m.broadcastLocked()
		return true
	}
	return false
}

// cancelWaiting cancels a not-yet-granted combination. It deliberately does
// not touch holds owned by an execution that has already started.
func (m *resourceManager) cancelWaiting(batchID, caseID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.waiters {
		w := &m.waiters[i]
		if w.res.BatchID == batchID && w.res.CaseID == caseID && !w.canceled {
			w.canceled = true
			m.broadcastLocked()
			return true
		}
	}
	return false
}

func (m *resourceManager) waitStatus() []WaitStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]WaitStatus, 0, len(m.waiters))
	for _, w := range m.waiters {
		if w.canceled {
			continue
		}
		status := WaitStatus{
			BatchID:   w.res.BatchID,
			CaseID:    w.res.CaseID,
			WorkerID:  w.res.WorkerID,
			Attempt:   w.res.Attempt,
			Requested: append([]string(nil), w.resources...),
		}
		for _, name := range w.resources {
			if holder, ok := m.holds[name]; ok {
				status.Missing = append(status.Missing, MissingResource{Resource: name, Holder: holder})
			}
		}
		out = append(out, status)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].BatchID != out[j].BatchID {
			return out[i].BatchID < out[j].BatchID
		}
		return out[i].CaseID < out[j].CaseID
	})
	return out
}

func (m *resourceManager) state() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return State{Reservations: m.snapshotLocked()}
}

func (m *resourceManager) restoreState(state State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	holds := make(map[string]Reservation, len(state.Reservations))
	for _, r := range state.Reservations {
		r.Resource = normalizeOne(r.Resource)
		r.Attempt = attemptOrDefault(r.Attempt)
		if r.Resource == "" || r.BatchID == "" || r.CaseID == "" {
			return errors.New("reservation is missing identity")
		}
		if _, exists := holds[r.Resource]; exists {
			return fmt.Errorf("duplicate restored reservation for %q", r.Resource)
		}
		if _, busy := m.holds[r.Resource]; busy {
			return fmt.Errorf("resource %q is already held", r.Resource)
		}
		holds[r.Resource] = r
	}
	for name, r := range holds {
		m.holds[name] = r
	}
	m.broadcastLocked()
	return nil
}

func (m *resourceManager) saveState() ([]byte, error) {
	m.mu.Lock()
	state := State{Reservations: m.snapshotLocked()}
	m.mu.Unlock()
	return json.Marshal(state)
}

func (m *resourceManager) loadState(data []byte) error {
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	return m.restoreState(state)
}

func (m *resourceManager) snapshotLocked() []Reservation {
	out := make([]Reservation, 0, len(m.holds))
	for _, r := range m.holds {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Resource < out[j].Resource })
	return out
}

// forceRelease drops uncertain holds owned by the given batch/case. Active,
// communicating executions are not treated as lost workers.
func (m *resourceManager) forceRelease(batchID, caseID string) []Reservation {
	m.mu.Lock()
	defer m.mu.Unlock()
	owner := batchID + "/" + caseID
	var released []Reservation
	for name, r := range m.holds {
		if r.owner() == owner && r.Uncertain {
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

// forceReleaseExecution drops exactly one execution's holds. The execution key
// prevents confirmation of an old attempt from releasing a later one.
func (m *resourceManager) forceReleaseExecution(res Reservation) []Reservation {
	m.mu.Lock()
	defer m.mu.Unlock()
	res.Attempt = attemptOrDefault(res.Attempt)
	key := res.executionKey()
	var released []Reservation
	for name, r := range m.holds {
		if r.executionKey() == key {
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
func (m *resourceManager) markUncertain(res Reservation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, r := range m.holds {
		if r.executionKey() == res.executionKey() {
			r.Uncertain = true
			m.holds[name] = r
		}
	}
}

func (m *resourceManager) snapshot() []Reservation {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked()
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

func normalizeResources(resources []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(resources))
	for _, name := range resources {
		name = normalizeOne(name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func normalizeOne(s string) string {
	for _, r := range s {
		if r != ' ' {
			return s
		}
	}
	return ""
}

func isSubset(smaller, larger []string) bool {
	next := 0
	for _, want := range smaller {
		for next < len(larger) && larger[next] < want {
			next++
		}
		if next >= len(larger) || larger[next] != want {
			return false
		}
	}
	return true
}

func attemptOrDefault(attempt int) int {
	if attempt <= 0 {
		return 1
	}
	return attempt
}
