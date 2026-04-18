package kpool

import (
	"errors"
	"hash/maphash"
	"sync"
)

// Pool is a concurrent worker pool that partitions work by key: items sharing
// a key are always routed to the same worker and processed in submission order.
// Different keys may run in parallel.
type Pool[K comparable, T any] struct {
	queues []chan T
	seed   maphash.Seed
	wg     sync.WaitGroup

	closeOnce sync.Once
	closedMx  sync.RWMutex
	closed    bool
}

// New returns a Pool with the given number of workers, each owning a buffered
// queue of size queueSize. Every accepted item is passed to handler.
// Returns an error if workers or queueSize is <= 0, or if handler is nil.
func New[K comparable, T any](
	workers int,
	queueSize int,
	handler func(T),
) (*Pool[K, T], error) {
	if workers <= 0 {
		return nil, errors.New("workers must be > 0")
	}
	if queueSize <= 0 {
		return nil, errors.New("queueSize must be > 0")
	}
	if handler == nil {
		return nil, errors.New("handler must not be nil")
	}

	p := &Pool[K, T]{
		queues: make([]chan T, 0, workers),
		seed:   maphash.MakeSeed(),
	}

	for range workers {
		queue := make(chan T, queueSize)

		p.wg.Go(func() {
			for msg := range queue {
				handler(msg)
			}
		})

		p.queues = append(p.queues, queue)
	}

	return p, nil
}

// Submit routes item to the worker queue selected by key, so items sharing a
// key are processed in submission order by a single worker. Blocks if that
// queue is full. Returns false if the pool has been closed.
//
// A true return only means the item was enqueued, not that it has been
// processed — processing happens asynchronously on a worker goroutine.
func (p *Pool[K, T]) Submit(key K, item T) bool {
	//nolint:gosec // G115: modulo by uint64(len(p.queues)) is always < len(p.queues), which fits in int.
	idx := int(maphash.Comparable(p.seed, key) % uint64(len(p.queues)))

	p.closedMx.RLock()
	defer p.closedMx.RUnlock()

	if p.closed {
		return false
	}

	p.queues[idx] <- item
	return true
}

// Close stops accepting new items, drains in-flight queues, and blocks until
// all workers have finished. Safe to call multiple times.
func (p *Pool[K, T]) Close() {
	p.closeOnce.Do(p.close)
}

func (p *Pool[K, T]) close() {
	p.closedMx.Lock()
	p.closed = true
	p.closedMx.Unlock()

	for _, q := range p.queues {
		close(q)
	}

	p.wg.Wait()
}
