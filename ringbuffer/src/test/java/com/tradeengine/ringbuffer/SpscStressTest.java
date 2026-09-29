package com.tradeengine.ringbuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Two real threads, millions of messages. Every message must arrive exactly
 * once, in order, and uncorrupted.
 *
 * <p>Each message carries three fields derived from its sequence number.
 * If the consumer ever saw a slot before the producer finished writing it
 * (a memory-ordering bug), the fields would disagree with each other. A
 * tiny buffer keeps it constantly full or empty, which is where those bugs
 * hide.
 */
class SpscStressTest {

    static final class Msg {
        long seq;
        long a;
        long b;
    }

    @ParameterizedTest(name = "capacity {0}, {1} messages")
    @CsvSource({"1024, 10000000", "2, 2000000", "1, 500000"})
    void everyMessageArrivesOnceInOrder(int capacity, long total) throws Exception {
        SpscRingBuffer<Msg> ring = new SpscRingBuffer<>(capacity, Msg::new);
        Checker checker = new Checker();

        Thread producer = new Thread(() -> {
            for (long seq = 0; seq < total; seq++) {
                Msg m;
                while ((m = ring.claim()) == null) {
                    Thread.onSpinWait();        // full: wait for the consumer
                }
                m.seq = seq;
                m.a = seq * 31;
                m.b = ~seq;
                ring.publish();
            }
        }, "producer");
        producer.setDaemon(true);
        producer.start();

        long deadline = System.nanoTime() + 60_000_000_000L;
        while (checker.received < total && checker.error == null) {
            if (ring.drain(checker, 256) == 0) {
                Thread.onSpinWait();            // empty: wait for the producer
            }
            if (System.nanoTime() > deadline) {
                fail("timed out after " + checker.received + " of " + total);
            }
        }
        producer.join(10_000);

        assertNull(checker.error, checker.error);
        assertEquals(total, checker.received);
        assertEquals(0, ring.size());
    }

    /** Runs on the consumer (test) thread; records the first problem it sees. */
    private static final class Checker implements EventHandler<Msg> {
        long received;
        String error;

        @Override
        public void onEvent(Msg m, long sequence) {
            if (error != null) {
                return;
            }
            // Read each field exactly once: re-reading for the error message
            // could show values the producer wrote after the check.
            long want = received;
            long s = m.seq;
            long a = m.a;
            long b = m.b;
            if (sequence != want || s != want) {
                error = "expected #" + want + " but got seq=" + sequence + " msg.seq=" + s;
            } else if (a != want * 31 || b != ~want) {
                error = "torn message #" + want + ": a=" + a + " (want " + want * 31
                        + ") b=" + b + " (want " + ~want + ")";
            }
            received++;
        }
    }
}
