package testsched

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Reservation is the audit record of one scarce device being held by one
// attempt. It survives batch boundaries: when a worker is lost the device
// stays reserved and the record keeps the evidence until a human confirms
// that the old execution has ended.
type Reservation struct {
	ID         string
	Resource   string
	BatchID    string
	CaseID     string
	Attempt    int
	WorkerID   int
	AcquiredAt time.Time
	// DetainedAt is set when the worker vanished while the case might still
	// be running. A detained reservation keeps the device occupied.
	DetainedAt time.Time
	ReleasedAt time.Time
	// Manual is true when the release came from an explicit human
	// confirmation rather than the case body finishing normally.
	Manual bool
	// Reason is the justification recorded with a manual release.
	Reason string
}

// Open reports whether the device is still occupied by this reservation.
func (r Reservation) Open() bool { return r.ReleasedAt.IsZero() }

// Detained reports whether the reservation is held because the worker was
// lost and awaits manual confirmation.
func (r Reservation) Detained() bool { return !r.DetainedAt.IsZero() && r.Open() }

// ResourceStat is a point-in-time accounting snapshot for one device.
type ResourceStat struct {
	Resource string
	// Held means the device is currently occupied (running or detained).
	Held bool
	// Detained means the holder is a lost worker awaiting confirmation.
	Detained bool
	// Waiting is the number of attempts queued for this device.
	Waiting int
}

type deviceManager struct {
	mu     sync.Mutex
	gates  map[string]*resourceGate
	ledger map[string]*Reservation
	order  []string
	seq    atomic.Int64
}

func newDeviceManager() *deviceManager {
	return &deviceManager{gates: map[string]*resourceGate{}, ledger: map[string]*Reservation{}}
}

// gateView snapshots the gate without holding the manager lock.
func (m *deviceManager) gate(resource string) *resourceGate {
	m.mu.Lock()
	g, ok := m.gates[resource]
	if !ok {
		g = &resourceGate{}
		m.gates[resource] = g
	}
	m.mu.Unlock()
	return g
}

func (m *deviceManager) acquire(
	ctx context.Context,
	batchID string,
	c Case,
	attemptNo, workerID int,
) (*lease, error) {
	l := &lease{m: m}
	now := time.Now
	for _, resource := range c.Resources {
		g := m.gate(resource)
		w := g.enqueue()
		granted := false
		select {
		case <-w.ch:
			granted = true
		case <-ctx.Done():
			granted = g.reclaim(w)
		}
		if !granted {
			l.release()
			return nil, ctx.Err()
		}
		id := fmt.Sprintf("res-%d", m.seq.Add(1))
		res := &Reservation{
			ID: id, Resource: resource, BatchID: batchID, CaseID: c.ID,
			Attempt: attemptNo, WorkerID: workerID, AcquiredAt: now(),
		}
		m.mu.Lock()
		m.ledger[id] = res
		m.order = append(m.order, id)
		m.mu.Unlock()
		l.items = append(l.items, leaseItem{gate: g, resource: resource, id: id, res: res})
	}
	return l, nil
}

func (m *deviceManager) markReleased(l *lease, manual bool, reason string) {
	m.mu.Lock()
	t := time.Now()
	for i := range l.items {
		item := &l.items[i]
		item.res.ReleasedAt = t
		item.res.Manual = manual
		item.res.Reason = reason
	}
	m.mu.Unlock()
}

func (m *deviceManager) markDetained(l *lease) {
	m.mu.Lock()
	t := time.Now()
	for i := range l.items {
		item := &l.items[i]
		if item.res.DetainedAt.IsZero() {
			item.res.DetainedAt = t
		}
	}
	m.mu.Unlock()
}

// ConfirmEnded releases a detained reservation after a human verifies the old
// execution no longer touches the device. The reservation, including the
// original uncertain attempt and the given reason, is retained in the ledger.
func (m *deviceManager) confirmEnded(id, reason string) (Reservation, bool) {
	m.mu.Lock()
	res, ok := m.ledger[id]
	if !ok || !res.Detained() {
		m.mu.Unlock()
		return Reservation{}, false
	}
	g := m.gates[res.Resource]
	if reason == "" {
		reason = "operator confirmed the old execution ended"
	}
	res.Manual = true
	res.Reason = reason
	res.ReleasedAt = time.Now()
	m.mu.Unlock()

	g.confirmRelease()
	return m.snapshot(id)
}

func (m *deviceManager) snapshot(id string) (Reservation, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	res, ok := m.ledger[id]
	if !ok {
		return Reservation{}, false
	}
	return *res, true
}

// Reservation returns the ledger entry for a reservation ID.
func (m *deviceManager) reservation(id string) (Reservation, bool) { return m.snapshot(id) }

// Detained returns every reservation still held by a lost worker.
func (m *deviceManager) detained() []Reservation {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []Reservation{}
	for _, id := range m.order {
		if m.ledger[id].Detained() {
			out = append(out, *m.ledger[id])
		}
	}
	return out
}

// Reservations returns the full append-only reservation ledger, in
// acquisition order.
func (m *deviceManager) reservations() []Reservation {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Reservation, 0, len(m.order))
	for _, id := range m.order {
		out = append(out, *m.ledger[id])
	}
	return out
}

func (m *deviceManager) stats() []ResourceStat {
	m.mu.Lock()
	names := make([]string, 0, len(m.gates))
	gates := make(map[string]*resourceGate, len(m.gates))
	for name, g := range m.gates {
		names = append(names, name)
		gates[name] = g
	}
	m.mu.Unlock()
	out := make([]ResourceStat, 0, len(names))
	for _, name := range names {
		held, detained, waiting := gates[name].snapshot()
		out = append(out, ResourceStat{Resource: name, Held: held, Detained: detained, Waiting: waiting})
	}
	return out
}

// lease represents the devices one attempt acquired.
type lease struct {
	m        *deviceManager
	items    []leaseItem
	detained atomic.Bool
}

type leaseItem struct {
	gate     *resourceGate
	resource string
	id       string
	res      *Reservation
}

func (l *lease) ids() []string {
	ids := make([]string, len(l.items))
	for i, item := range l.items {
		ids[i] = item.id
	}
	return ids
}

func (l *lease) resources() []string {
	names := make([]string, len(l.items))
	for i, item := range l.items {
		names[i] = item.resource
	}
	return names
}

// release frees every device after a normal attempt completion and hands it
// to the next waiter in arrival order.
func (l *lease) release() {
	for i := len(l.items) - 1; i >= 0; i-- {
		l.items[i].gate.release()
	}
	l.m.markReleased(l, false, "")
}

// detain keeps every device occupied after a worker is lost. Waiters (in any
// batch) stay blocked until confirmRelease runs for the device.
func (l *lease) detain() {
	for i := len(l.items) - 1; i >= 0; i-- {
		l.items[i].gate.detain()
	}
	l.m.markDetained(l)
}

// waiter is one queued attempt. The buffered channel carries one token when
// the device has been granted to this waiter.
type waiter struct {
	ch       chan struct{}
	canceled bool
}

// resourceGate is a fair, FIFO lock for one device shared across batches.
type resourceGate struct {
	mu       sync.Mutex
	held     bool
	detained bool
	waiters  []*waiter
}

func (g *resourceGate) enqueue() *waiter {
	w := &waiter{ch: make(chan struct{}, 1)}
	g.mu.Lock()
	g.waiters = append(g.waiters, w)
	if !g.held {
		g.grantNextLocked()
	}
	g.mu.Unlock()
	return w
}

// reclaim handles a waiter whose context ended. If the token had already been
// granted it reports true so the caller treats the device as acquired.
func (g *resourceGate) reclaim(w *waiter) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-w.ch:
		return true
	default:
	}
	w.canceled = true
	if len(g.waiters) > 0 && g.waiters[0] == w {
		g.waiters = g.waiters[1:]
		if !g.held {
			g.grantNextLocked()
		}
	}
	return false
}

// release ends a normal holding period and promotes the next waiter.
func (g *resourceGate) release() {
	g.mu.Lock()
	if len(g.waiters) > 0 {
		g.waiters = g.waiters[1:]
	}
	g.held = false
	g.detained = false
	g.grantNextLocked()
	g.mu.Unlock()
}

// detain ends the holder's queue slot but keeps the device occupied, so no
// waiter is promoted. confirmRelease is the only way forward.
func (g *resourceGate) detain() {
	g.mu.Lock()
	if len(g.waiters) > 0 {
		g.waiters = g.waiters[1:]
	}
	g.held = true
	g.detained = true
	g.mu.Unlock()
}

// confirmRelease marks the manually confirmed release and promotes the next
// waiter.
func (g *resourceGate) confirmRelease() {
	g.mu.Lock()
	g.held = false
	g.detained = false
	g.grantNextLocked()
	g.mu.Unlock()
}

func (g *resourceGate) grantNextLocked() {
	for len(g.waiters) > 0 {
		w := g.waiters[0]
		if w.canceled {
			g.waiters = g.waiters[1:]
			continue
		}
		g.held = true
		g.detained = false
		w.ch <- struct{}{}
		return
	}
	g.held = false
}

func (g *resourceGate) snapshot() (held, detained bool, waiting int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	held = g.held
	detained = g.detained
	waiting = len(g.waiters)
	return
}
