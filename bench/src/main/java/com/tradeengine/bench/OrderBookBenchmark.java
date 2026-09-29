package com.tradeengine.bench;

import static com.tradeengine.orderbook.OrderType.IOC;
import static com.tradeengine.orderbook.OrderType.LIMIT;
import static com.tradeengine.orderbook.Side.BUY;
import static com.tradeengine.orderbook.Side.SELL;

import com.tradeengine.orderbook.OrderBook;
import java.util.Random;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Cost of one book operation, in steady state.
 *
 * <p>Each benchmark leaves the book exactly as it found it, so millions of
 * iterations measure the same situation instead of a book that slowly
 * fills up or drains. The book starts with 50 price levels of liquidity
 * on each side, 20 orders per level, and an empty price in the middle.
 *
 * <p>Two modes: AverageTime gives ns/op, SampleTime gives the latency
 * distribution (p50, p99, p99.9). Run with {@code -prof gc} to see bytes
 * allocated per operation, which should be ~0.
 */
@State(Scope.Thread)
@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
public class OrderBookBenchmark {

    private static final long BASE = 100_000;
    private static final int LEVELS = 2_000;
    private static final long MID = BASE + LEVELS / 2;     // kept empty
    private static final int DEPTH = 50;
    private static final int ORDERS_PER_LEVEL = 20;
    private static final int PRICES = 1024;                // power of 2 for masking

    private CountingListener listener;
    private OrderBook book;
    private long[] bidPrices;
    private int cursor;
    private long clOrdId;

    @Setup(Level.Trial)
    public void setUp() {
        listener = new CountingListener();
        book = new OrderBook(1, BASE, LEVELS, 100_000, listener);
        for (int level = 1; level <= DEPTH; level++) {
            for (int n = 0; n < ORDERS_PER_LEVEL; n++) {
                book.newOrder(1, ++clOrdId, BUY, LIMIT, MID - level, 100, 0);
                book.newOrder(1, ++clOrdId, SELL, LIMIT, MID + level, 100, 0);
            }
        }
        // Pre-generated so random() isn't part of what we measure.
        Random rnd = new Random(1);
        bidPrices = new long[PRICES];
        for (int i = 0; i < PRICES; i++) {
            bidPrices[i] = MID - 1 - rnd.nextInt(DEPTH);
        }
    }

    /**
     * Rest a limit order inside the existing bids, then cancel it. The
     * common case on a real exchange: most orders are cancelled, not filled.
     */
    @Benchmark
    @OperationsPerInvocation(2)
    public long addThenCancel() {
        long price = bidPrices[cursor++ & (PRICES - 1)];
        long id = book.newOrder(1, ++clOrdId, BUY, LIMIT, price, 10, 0);
        book.cancel(clOrdId, id);
        return id;
    }

    /**
     * Rest a sell at the empty middle price (it becomes best ask), then hit
     * it with a buy IOC: one full trade, the level empties, best ask moves
     * back out one tick. Exercises the whole match path.
     */
    @Benchmark
    @OperationsPerInvocation(2)
    public long restThenFill() {
        book.newOrder(1, ++clOrdId, SELL, LIMIT, MID, 10, 0);
        book.newOrder(1, ++clOrdId, BUY, IOC, MID, 10, 0);
        return listener.trades;
    }

    @TearDown(Level.Trial)
    public void checkSteadyState() {
        // If a benchmark leaked orders or got rejected, the numbers are lies.
        int expected = 2 * DEPTH * ORDERS_PER_LEVEL;
        if (book.liveOrders() != expected || listener.rejected != 0) {
            throw new IllegalStateException("book drifted: live=" + book.liveOrders()
                    + " expected=" + expected + " rejected=" + listener.rejected);
        }
    }
}
