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
| 3 | TCP gateway, risk checks, end-to-end latency | ✅ 3 server processes, TCP gateway, risk checks, kill -9 failover, end-to-end latency |

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

### End to end: 3 server processes, real TCP, real fsync

`scripts/cluster.sh` starts three JVMs on one laptop and a load generator
that sends at a fixed rate and records order → ack latency (HdrHistogram,
measured from each order's scheduled send time, so stalls can't hide).

| Load | Acked | p50 | p99 | p99.9 |
|---|---|---|---|---|
| 1,000 orders/s | 25,000 / 25,000 | 49 ms | 66 ms | 78 ms |
| 10,000 orders/s | 250,000 / 250,000 | 47 ms | 75 ms | 89 ms |
| 50,000 orders/s | 1,250,000 / 1,250,000 | 61 ms | 95 ms | 171 ms |
| 100,000 orders/s | 2,500,000 / 2,500,000 | 492 ms (saturated) | 722 ms | 763 ms |
| 200,000 orders/s | can't keep up | | | |

**Leader killed with `kill -9` at 10,000 orders/s:** all 250,000 orders
acknowledged, none lost or doubled (the failover test proves this by count),
clients reconnected on their own, trading paused **~0.3 s** (281–318 ms
across runs). With snapshots every 100,000 entries, each node's disk holds
a small snapshot plus only the log since it, not every order ever placed.

**Where the time goes** (same cluster, one order at a time):

| | p50 |
|---|---|
| No fsync | **0.10 ms** |
| With fsync | 11.2 ms |
| One fsync on this Mac | 3.9 ms |

The code path through gateway, Raft over TCP and the book takes about
100 µs; nearly all of the rest is the leader's and a follower's fsync. Under
load the three processes also share one SSD, so their fsyncs queue behind
each other; on three real machines each node has its own disk.

**How the numbers got here:** the first run managed a 50 ms median at
1,000/s and collapsed at 10,000/s (leaders kept changing). Two causes: a
follower did one fsync per message instead of one per event-loop turn, and
the leader re-sent every in-flight entry each turn instead of pipelining.
After fixing both (one fsync per turn, before any byte leaves the process;
`nextIndex` advances on send), 10,000/s went from collapse to every order
acknowledged, and the chaos and Figure 8 tests still pass.

**Experiment: fsync on its own thread** (`--async-fsync true`). The event
loop hands the fsync to a background thread and keeps working; Raft replies
are held back until their turn's fsync finishes. Measured, it was slower:

| Load | Blocking fsync p50 / p99 | Async fsync p50 / p99 |
|---|---|---|
| 1,000/s | 49 / 66 ms | 63 / 88 ms |
| 10,000/s | 47 / 75 ms | 63 / 93 ms |
| 50,000/s | 61 / 95 ms | 69 / 206 ms |

Why: with blocking fsync a turn's writes are flushed straight away, one
fsync of waiting. With the thread, they usually arrive while the previous
fsync is already running, so they wait for it and then for their own: up
to two fsyncs, at the leader and again at the follower. Freeing the loop
only pays when the loop has other work; here the time is almost all one
shared disk. So blocking fsync stays the default. What would beat it is
Raft §10.2.1: the leader sends entries to followers while its own fsync
runs, and counts itself toward a majority only once that fsync is done.
That needs a simulator that really loses unsynced writes on a crash
before it can be called safe, so it's left as the next step, along with
Linux + NVMe and a multi-connection load generator.

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
- **Snapshots and log compaction:** every N entries a node saves its whole
  state and deletes the log it covers; a follower too far behind gets the
  snapshot (InstallSnapshot) instead of the history; a restart loads the
  snapshot first. The 100-seed chaos simulation runs with snapshots every 20
  entries, a restored order book must behave exactly like the original under
  20,000 further random orders, and a crash between writing the snapshot and
  rewriting the log is recovered on the next start. Injected bugs (restore
  skipped, off-by-one on the overlap, wrong term, dedup state forgotten) are
  all caught.
- **Failover over TCP:** a client streams 3,000 orders to three real
  servers and the leader is shut down a third of the way through. The
  client follows NOT_LEADER, reconnects and resends; afterwards every node's
  book holds exactly 3,000 orders (fewer would mean loss, more duplication).
  With dedup switched off it reads 3,256.
- **Gateway over TCP:** fills reach both sides, risk checks reject before
  the log, followers redirect, and a client can't cancel another client's
  order.

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
- [Snapshots and async fsync LLD](docs/lld-snapshots.html): log
  compaction, InstallSnapshot, fsync off the event-loop thread

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
server/      event-loop server: TCP framing, Raft over TCP, gateway,
             failover client, load generator (java -jar trade-engine.jar)
scripts/     cluster.sh: run 3 nodes + load generator, optionally kill -9 the leader
bench/       JMH benchmarks
docs/        HLD, design decisions, LLD
```
