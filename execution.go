package testsched

import "sync/atomic"

// Execution is the per-attempt handle passed to a case body.
type Execution struct {
	workerID int
	attempt  int
	lost     chan struct{}
	done     atomic.Bool
}

// WorkerID identifies the worker executing this attempt.
func (e *Execution) WorkerID() int { return e.workerID }

// Attempt is the 1-based attempt number within this batch.
func (e *Execution) Attempt() int { return e.attempt }

// LoseWorker simulates loss of the worker process/connection: the current
// attempt produces no result and is recorded as uncertain. The scheduler
// replaces the lost worker so remaining concurrency is preserved.
func (e *Execution) LoseWorker() {
	if e.done.CompareAndSwap(false, true) {
		close(e.lost)
	}
}
