# trade-engine
Low-latency order matching engine in Java with Raft replication and zero-GC design

A stock exchange core: clients send buy and sell orders, the engine
matches them by **price-time priority**, and reports every trade. The
goal is microsecond latency with no garbage collection on the hot path,
then fault tolerance through Raft replication.

## Status

| Tier | Scope | State |
|---|---|---|
| 1 | Order book, lock-free ring buffer, binary market data feed, benchmarks | 🟡 order book + ring buffer done, feed next |
| 2 | Raft-replicated sequencer, failover, deterministic replay | ⬜ |
| 3 | TCP gateway, risk checks, end-to-end latency | ⬜ |

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

Key choices in the order book:
- Prices are whole ticks in a `long`, never `double`.
- Each side is an array indexed by price, so add, cancel and best price
  are all O(1).
- Orders at one price form an intrusive linked list (oldest first), so
  cancel unlinks in O(1) with no searching.
- One thread owns the book: no locks anywhere.

## Layout

```
orderbook/   matching engine + tests
ringbuffer/  lock-free SPSC ring buffer + stress tests
bench/       JMH benchmarks
docs/        HLD, design decisions, LLD
```
