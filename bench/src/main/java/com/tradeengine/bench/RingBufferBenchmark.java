package com.tradeengine.bench;

import com.tradeengine.ringbuffer.EventHandler;
import com.tradeengine.ringbuffer.SpscRingBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Control;

/**
 * One producer thread, one consumer thread, messages passed as fast as
 * possible. Ours vs {@link ArrayBlockingQueue}, same capacity, both spinning
 * instead of blocking, so the difference is the queue itself.
 *
 * <p>Read the {@code produce} rows: messages per microsecond that made it
 * through. Every spin loop checks {@link Control#stopMeasurement} so a thread
 * waiting on a full or empty queue can't hang the end of an iteration.
 */
@State(Scope.Group)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 3, jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
public class RingBufferBenchmark {

    private static final int CAPACITY = 1024;

    /** Same shape as a small order command. */
    public static final class Msg {
        long orderId;
        long price;
        long qty;
    }

    private SpscRingBuffer<Msg> ring;
    private ArrayBlockingQueue<Msg> abq;
    private Msg shared;             // ABQ passes a reference: no allocation either
    private long produced;
    private long consumedSum;
    private final EventHandler<Msg> sink = (m, seq) -> consumedSum += m.qty;

    @Setup
    public void setUp() {
        ring = new SpscRingBuffer<>(CAPACITY, Msg::new);
        abq = new ArrayBlockingQueue<>(CAPACITY);
        shared = new Msg();
        shared.qty = 1;
    }

    // --- ours ---

    @Benchmark
    @Group("spsc")
    @GroupThreads(1)
    public void produce(Control control) {
        Msg m;
        while ((m = ring.claim()) == null) {
            if (control.stopMeasurement) {
                return;
            }
            Thread.onSpinWait();
        }
        m.orderId = ++produced;
        m.price = 100_050;
        m.qty = 1;
        ring.publish();
    }

    @Benchmark
    @Group("spsc")
    @GroupThreads(1)
    public long consume(Control control) {
        while (ring.drain(sink, 64) == 0) {
            if (control.stopMeasurement) {
                break;
            }
            Thread.onSpinWait();
        }
        return consumedSum;
    }

    // --- ArrayBlockingQueue ---

    @Benchmark
    @Group("abq")
    @GroupThreads(1)
    public void abqProduce(Control control) {
        while (!abq.offer(shared)) {
            if (control.stopMeasurement) {
                return;
            }
            Thread.onSpinWait();
        }
    }

    @Benchmark
    @Group("abq")
    @GroupThreads(1)
    public long abqConsume(Control control) {
        Msg m;
        while ((m = abq.poll()) == null) {
            if (control.stopMeasurement) {
                return consumedSum;
            }
            Thread.onSpinWait();
        }
        consumedSum += m.qty;
        return consumedSum;
    }
}
