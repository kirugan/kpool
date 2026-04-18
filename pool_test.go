package kpool_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kirugan/kpool"
)

var newPool = kpool.New[int, int]

func TestNew(t *testing.T) {
	cases := []struct {
		name      string
		workers   int
		queueSize int
		handler   func(int)
		wantErr   bool
	}{
		{"invalid workers", 0, 1, func(int) {}, true},
		{"invalid queueSize", 1, 0, func(int) {}, true},
		{"nil handler", 1, 1, nil, true},
		{"happy path", 4, 8, func(int) {}, false},
	}

	for _, tc := range cases {
		p, err := newPool(tc.workers, tc.queueSize, tc.handler)
		if tc.wantErr {
			if err == nil {
				t.Errorf("%s: expected error, got nil", tc.name)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
			continue
		}
		p.Close()
	}
}

func TestSubmit(t *testing.T) {
	t.Parallel()

	t.Run("same key preserves order", func(t *testing.T) {
		t.Parallel()

		const n = 1000
		var mu sync.Mutex
		got := make([]int, 0, n)

		p, err := newPool(8, 32, func(v int) {
			mu.Lock()
			got = append(got, v)
			mu.Unlock()
		})
		if err != nil {
			t.Fatal(err)
		}

		for i := range n {
			if !p.Submit(0, i) {
				t.Fatalf("submit %d returned false", i)
			}
		}
		p.Close()

		if len(got) != n {
			t.Fatalf("processed %d items, want %d", len(got), n)
		}
		for i, v := range got {
			if v != i {
				t.Fatalf("out of order at index %d: got %d, want %d", i, v, i)
			}
		}
	})

	t.Run("after close returns false", func(t *testing.T) {
		t.Parallel()

		p, err := newPool(1, 1, func(int) {})
		if err != nil {
			t.Fatal(err)
		}
		p.Close()

		if p.Submit(0, 1) {
			t.Fatal("Submit after Close returned true, want false")
		}
	})
}

func TestClose(t *testing.T) {
	t.Parallel()

	t.Run("idempotent", func(t *testing.T) {
		t.Parallel()

		p, err := newPool(2, 4, func(int) {})
		if err != nil {
			t.Fatal(err)
		}
		p.Close()
		p.Close() // must not panic or block
	})

	t.Run("drains in flight", func(t *testing.T) {
		t.Parallel()

		const n = 200
		var processed atomic.Int32

		p, err := newPool(4, 64, func(int) {
			processed.Add(1)
		})
		if err != nil {
			t.Fatal(err)
		}

		for i := range n {
			if !p.Submit(i, i) {
				t.Fatalf("submit %d returned false", i)
			}
		}
		p.Close()

		if got := processed.Load(); got != n {
			t.Fatalf("processed %d, want %d", got, n)
		}
	})
}

func TestDifferentKeysRunInParallel(t *testing.T) {
	t.Parallel()

	const workers = 4
	const keys = 64

	var active, maxActive atomic.Int32

	p, err := newPool(workers, 16, func(int) {
		cur := active.Add(1)
		for {
			m := maxActive.Load()
			if cur <= m || maxActive.CompareAndSwap(m, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		active.Add(-1)
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := range keys {
		p.Submit(i, i)
	}
	p.Close()

	if got := maxActive.Load(); got < 2 {
		t.Fatalf("max concurrency = %d, want >= 2", got)
	}
}
