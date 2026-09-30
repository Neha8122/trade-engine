# trade-engine
Low-latency order matching engine in Java with Raft replication and zero-GC design

A stock exchange core: clients send buy and sell orders, the engine
matches them by **price-time priority**, and reports every trade. The
goal is microsecond latency with no garbage collection on the hot path,
then fault tolerance through Raft replication.

## Status

| Tier | Scope | State |
|---|---|---|
| 1 | Order book, lock-free ring buffer, binary market data feed, benchmarks | ✅ order book, ring buffer, market data feed |
| 2 | Raft-replicated sequencer, failover, deterministic replay | ✅ replicated order book on Raft, durable storage, group commit (snapshots + TCP later) |
| 3 | TCP gateway, risk checks, end-to-end latency | 🟡 wire protocol + event loop + Raft over TCP done; gateway next |

## Results so far

### Order book (single thread)

| Operation | Mean time per operation | Allocation |
|---|---|---|
| Add a resting limit order, or cancel one | **~13–15 ns** | **0 B** |
| Rest an order and fully fill it with an incoming order (one trade) | **~20 ns** | **0 B** |

- **No garbage collection during the measured runs** (`gc.count ≈ 0`,
  about 10⁻⁴ bytes allocated per operation): orders come from a
  preallocated pool and ids are looked up in a primitive hash map.
- **Tail latency** (JMH sample mode): p99.9 ≈ 83 ns, p99.99 ≈ 2–3 µs.
  The rare µs outliers are OS and JIT jitter, not GC. macOS timer
  resolution is about 40 ns, so percentiles below that are coarse.
- **Setup:** Apple M1 (8 cores), 16 GB, macOS 26, OpenJDK 21.0.8, 1 GB
  heap, default G1 GC, no CPU pinning, laptop with other apps running.
  These are in-process microbenchmarks of the matching core only, not
  end-to-end network latency.

### Lock-free ring buffer (one producer thread → one consumer thread)

| Queue | Messages per second | Allocation per message |
|---|---|---|
| `ArrayBlockingQueue` (JDK) | ~6 million | ~6 B, GC ran |
| **`SpscRingBuffer` (this repo)** | **~24 million (~4×)** | **0 B, no GC** |

Same capacity (1024), same message, both sides spin instead of
blocking, so the difference is the queue itself. This is throughput,
not one-way latency. The stress test that guards it catches a real
memory-ordering bug: with plain writes instead of release/acquire, the
consumer on this ARM (M1) machine read half-written messages within
the first ~30,000.

### Market data feed

| Operation | Mean time | Allocation |
|---|---|---|
| Encode one feed message (into the packet + retransmit store) | **~7 ns** | **0 B** |
| Rest + fill, feed off → feed on | **~16 ns → ~26 ns** per operation | **0 B** |

Over a fake network that drops ~5%, delays ~5% and duplicates ~3% of
packets, a subscriber's rebuilt book matches the real book exactly at
every price level after 30,000 random orders, by fetching missed
messages using sequence numbers. With recovery off it doesn't.

### Replicated exchange: what group commit buys

Cost per order to get it committed on a 3-node Raft cluster and applied on
the leader, i.e. safe to acknowledge (µs per order):

| Orders per batch | In memory | Files + fsync |
|---|---|---|
| 1 | ~1.6 (noisy) | **11.9** |
| 8 | 0.18 | **1.40** |
| 64 | 0.044 | **0.19** |
| 512 | ~0.05 (noisy) | **0.047** |

With one order per batch every order pays for three fsyncs (leader and
both followers). Batching spreads them over many orders: on real files the
cost per order falls ~250×, and at 512 per batch storage stops mattering.

Caveats: all three nodes run in one thread with an instant network, so the
three fsyncs happen one after another rather than in parallel, and there's
no network round trip. On macOS `fsync` doesn't flush the drive's own cache,
so absolute numbers are optimistic; on Linux with a real flush each fsync
costs more, which makes batching matter even more. The ratio is the point.

### Fix found by a benchmark
When the last order on one side was removed, finding the next best price
checked every empty price level one by one. A bitmap of non-empty levels
now checks 64 levels per step:

| Worst case: fill the only ask, ask side becomes empty | Mean time |
|---|---|
| Before (scan ~1,000 empty levels) | ~132 ns |
| **After (bitmap, ~16 word checks)** | **~20 ns** |

Reproduce:
```
mvn package
java -jar bench/target/benchmarks.jar -prof gc
```

## How it's tested

- **Unit and scenario tests** for every edge case: IOC and market
  leftovers, same-price FIFO, sweeping several price levels, cancels,
  pool exhaustion, and a no-leak check that every order returns to the pool.
- **Differential test:** 100,000 random orders and cancels run through the
  fast book and a simple `TreeMap` reference book; every event must match
  at every step. It catches bugs the hand-written tests miss.
- **Determinism test:** the same command sequence always produces the same
  events, which Raft replication depends on.
- **Ring buffer stress test:** two real threads pass 10 million messages;
  each must arrive once, in order, with every field intact. Runs with
  capacities 1024, 2 and 1 so full and empty are hit constantly.
- **Feed over a lossy network:** real book → publisher → fake network
  that drops, duplicates and reorders packets → subscriber replica, which
  must equal the real book at every price level. Plus a real UDP socket
  test on localhost.
- **Raft under simulated chaos:** 200 seeded runs of 3–5 nodes with 10%
  message loss, reordering, random partitions, crashes and restarts. After
  every step the simulator checks election safety, log matching, leader
  completeness, state machine safety and that commit indexes never go
  down; at the end every node must have applied the identical log. Four
  deliberately injected Raft bugs (voting for a stale log, forgetting a
  vote, committing an old-term entry by counting replicas, and letting the
  commit index move backwards) are each caught. The paper's Figure 8
  scenario has its own step-by-step test.
- **Durable storage:** term, vote and log live in checksummed files and
  are fsynced before a node replies. Tests cut a write short and corrupt
  bytes on purpose: a torn last record is dropped, a damaged meta slot
  falls back to the previous one, and a node restarted from disk refuses
  to vote twice in a term. The chaos simulation also runs on real files,
  with crashed nodes reloading everything from disk.
- **Replicated order books under chaos:** 33 seeded runs (3 on real files)
  where clients send orders to whoever leads and resend anything not
  acknowledged, while nodes crash, restart and get partitioned. Every book
  must produce identical events, every acknowledged order must reach the
  book (none lost), and every order must be applied exactly once however
  often it was resent. Injected bugs (no dedup, acking before commit,
  acking an order that a new leader overwrote) are all caught.

```
mvn test
```

## Design

- [HLD](docs/HLD.md): system design in interview order, with a
  [diagram version](docs/hld.html)
- [Design decisions](docs/DESIGN.md): the reasoning behind each choice
- [Order book LLD](docs/lld-orderbook.html): classes, memory layout,
  matching flowchart and a worked example
- [Ring buffer LLD](docs/lld-ringbuffer.html): lock-free handoff between
  threads, false sharing, release/acquire ordering
- [Market data feed LLD](docs/lld-feed.html): binary packets, UDP
  multicast, sequence-number gap recovery
- [Raft replication LLD](docs/lld-raft.html): roles, log replication,
  safety rules, and the simulation-first code design
- [Gateway and server LLD](docs/lld-gateway.html): event loop, TCP
  protocol, risk checks, end-to-end latency measurement

Key choices in the order book:
- Prices are whole ticks in a `long`, never `double`.
- Each side is an array indexed by price, so add, cancel and best price
  are all O(1). A bitmap of non-empty levels finds the next best price
  64 levels at a time when the best level empties.
- Orders at one price form an intrusive linked list (oldest first), so
  cancel unlinks in O(1) with no searching.
- One thread owns the book: no locks anywhere.

## Layout

```
orderbook/   matching engine + tests
ringbuffer/  lock-free SPSC ring buffer + stress tests
feed/        binary market data feed over UDP, gap recovery
raft/        Raft consensus core + deterministic cluster simulator
exchange/    order book as Raft's state machine: sequencer, acks, dedup
server/      event-loop server: TCP framing, Raft over TCP, gateway
bench/       JMH benchmarks
docs/        HLD, design decisions, LLD
```
