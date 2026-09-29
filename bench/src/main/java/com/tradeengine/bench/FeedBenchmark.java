package com.tradeengine.bench;

import static com.tradeengine.orderbook.OrderType.IOC;
import static com.tradeengine.orderbook.OrderType.LIMIT;
import static com.tradeengine.orderbook.Side.BUY;
import static com.tradeengine.orderbook.Side.SELL;

import com.tradeengine.feed.MarketDataPublisher;
import com.tradeengine.feed.Packetizer;
import com.tradeengine.feed.PacketSink;
import com.tradeengine.feed.RetransmitStore;
import com.tradeengine.orderbook.OrderBook;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * What the market data feed adds to the engine: encoding each public event
 * into the packet buffer and copying it into the retransmit store. The sink
 * only counts bytes, so this measures our code, not the OS network stack.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 3, jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
public class FeedBenchmark {

    private static final long BASE = 100_000;
    private static final long MID = BASE + 1_000;

    private MarketDataPublisher publisher;
    private OrderBook plainBook;
    private OrderBook feedBook;
    private long orderId;
    private long clOrdId;
    long bytesSent;

    @Setup(Level.Trial)
    public void setUp() {
        PacketSink countingSink = (ByteBuffer p) -> bytesSent += p.remaining();
        publisher = new MarketDataPublisher(
                new Packetizer(Packetizer.DEFAULT_MAX_PACKET, countingSink, new RetransmitStore(1 << 16)));
        plainBook = new OrderBook(1, BASE, 2_000, 10_000, new CountingListener());
        feedBook = new OrderBook(1, BASE, 2_000, 10_000, new CountingListener(), publisher);
        // Same liquidity as OrderBookBenchmark: 50 levels each side around an
        // empty middle price, so the numbers are comparable.
        for (OrderBook book : new OrderBook[] {plainBook, feedBook}) {
            for (int level = 1; level <= 50; level++) {
                for (int n = 0; n < 20; n++) {
                    book.newOrder(1, ++clOrdId, BUY, LIMIT, MID - level, 100, 0);
                    book.newOrder(1, ++clOrdId, SELL, LIMIT, MID + level, 100, 0);
                }
            }
        }
        publisher.flush();
    }

    /** Encode one AddOrder and one OrderDeleted. */
    @Benchmark
    @OperationsPerInvocation(2)
    public long encodeAddAndDelete() {
        long id = ++orderId;
        publisher.onAdd(id, BUY, MID, 10);
        publisher.onDelete(id);
        return bytesSent;
    }

    /** Baseline: rest a sell, fill it with an IOC. No feed attached. */
    @Benchmark
    @OperationsPerInvocation(2)
    public long restThenFillNoFeed() {
        plainBook.newOrder(1, ++clOrdId, SELL, LIMIT, MID, 10, 0);
        plainBook.newOrder(1, ++clOrdId, BUY, IOC, MID, 10, 0);
        return clOrdId;
    }

    /** Same, with the feed publishing the AddOrder and OrderExecuted. */
    @Benchmark
    @OperationsPerInvocation(2)
    public long restThenFillWithFeed() {
        feedBook.newOrder(1, ++clOrdId, SELL, LIMIT, MID, 10, 0);
        feedBook.newOrder(1, ++clOrdId, BUY, IOC, MID, 10, 0);
        publisher.flush();
        return bytesSent;
    }
}
