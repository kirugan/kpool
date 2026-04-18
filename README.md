# kpool

[![Go Reference](https://pkg.go.dev/badge/github.com/kirugan/kpool.svg)](https://pkg.go.dev/github.com/kirugan/kpool)
[![Go Report Card](https://goreportcard.com/badge/github.com/kirugan/kpool)](https://goreportcard.com/report/github.com/kirugan/kpool)

A generic, keyed worker pool for Go.

> No, not KPop 🎤💃. **KPool** 🪣⚙️. Sorry.

## What it does

`kpool.Pool[K, T]` is a worker pool that partitions work by key: every item you `Submit` is routed to a fixed worker based on its key, so **items sharing a key are always processed by the same worker, in submission order**. Items with different keys can run in parallel.

Think of `K` as a Kafka partition key — but local to a single process. Same idea: consistent routing + per-key ordering, without the broker.

## Why

Imagine you're processing messages from users (a bot, an API, whatever), and each user's messages must be handled **strictly sequentially** — otherwise two concurrent handlers for the same user will step on each other.

The obvious solution is a `map[userID]*sync.Mutex` — but now you have to worry about:

- how and when to clean up mutexes for idle users
- memory growth if the set of users is unbounded
- the lock contention around the map itself

`kpool` sidesteps all of that. You pick `workers` (say 32), and every user is deterministically hashed to one of them. That user's messages queue up behind that one worker and run one at a time — for free, no mutexes, no cleanup. Other users route to other workers and run concurrently.

Trade-off: multiple users share a worker, so a slow user can delay other users that happen to hash to the same slot. Size `workers` accordingly.

## Install

Requires Go 1.26 or later.

```bash
go get github.com/kirugan/kpool
```

## Usage

```go
p, err := kpool.New[string, Message](32, 128, func(m Message) {
    process(m) // called sequentially per UserID
})
if err != nil {
    log.Fatal(err)
}
defer p.Close()

p.Submit(msg.UserID, msg)
```

See the [runnable example on pkg.go.dev](https://pkg.go.dev/github.com/kirugan/kpool#example-package).

## API

- `New[K, T](workers, queueSize, handler)` — build a pool. `handler` is called once per item.
- `Submit(key K, item T) bool` — enqueue. Blocks if the target worker's queue is full. Returns `false` if the pool is closed. A `true` return means enqueued, **not** processed.
- `Close()` — stop accepting new items, drain in-flight queues, wait for workers. Safe to call multiple times.

## Development

```bash
make lint    # run golangci-lint
make format  # auto-fix formatting
make test    # go test ./...
```

## License

MIT — see [LICENSE](LICENSE).
