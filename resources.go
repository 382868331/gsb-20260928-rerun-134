package testsched

import (
	"context"
	"sync"
)

// resourceLocks serializes cases that declare the same shared resource.
// Locks are always acquired in sorted order, so multi-resource cases cannot
// deadlock against each other.
type resourceLocks struct {
	mu         sync.Mutex
	semaphores map[string]chan struct{}
}

func newResourceLocks() *resourceLocks {
	return &resourceLocks{semaphores: map[string]chan struct{}{}}
}

func (l *resourceLocks) acquire(ctx context.Context, resources []string) (release func()) {
	acquired := make([]chan struct{}, 0, len(resources))
	for _, name := range resources {
		l.mu.Lock()
		sem, ok := l.semaphores[name]
		if !ok {
			sem = make(chan struct{}, 1)
			l.semaphores[name] = sem
		}
		l.mu.Unlock()
		select {
		case sem <- struct{}{}:
			acquired = append(acquired, sem)
		case <-ctx.Done():
			return l.releaseFunc(acquired)
		}
	}
	return l.releaseFunc(acquired)
}

func (l *resourceLocks) releaseFunc(acquired []chan struct{}) func() {
	return func() {
		for i := len(acquired) - 1; i >= 0; i-- {
			<-acquired[i]
		}
	}
}
