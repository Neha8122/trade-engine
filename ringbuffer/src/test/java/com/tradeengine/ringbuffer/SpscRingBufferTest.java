package com.tradeengine.ringbuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Single-thread behaviour: empty, full, wrap-around, batching, misuse. */
class SpscRingBufferTest {

    /** A reusable slot, like an order command in the real pipeline. */
    static final class Msg {
        long value;
    }

    private final SpscRingBuffer<Msg> ring = new SpscRingBuffer<>(4, Msg::new);
    private final List<Long> values = new ArrayList<>();
    private final List<Long> sequences = new ArrayList<>();
    private final EventHandler<Msg> collect = (m, seq) -> {
        values.add(m.value);
        sequences.add(seq);
    };

    @Test
    void capacityMustBePowerOfTwo() {
        assertThrows(IllegalArgumentException.class, () -> new SpscRingBuffer<>(3, Msg::new));
        assertThrows(IllegalArgumentException.class, () -> new SpscRingBuffer<>(0, Msg::new));
        assertThrows(IllegalArgumentException.class, () -> new SpscRingBuffer<>(-4, Msg::new));
        assertEquals(1, new SpscRingBuffer<>(1, Msg::new).capacity());
    }

    @Test
    void emptyBufferDrainsNothing() {
        assertEquals(0, ring.drain(collect, 10));
        assertEquals(0, ring.size());
    }

    @Test
    void messagesComeOutInOrderWithSequences() {
        send(10, 20, 30);

        assertEquals(3, ring.drain(collect, 10));
        assertEquals(List.of(10L, 20L, 30L), values);
        assertEquals(List.of(0L, 1L, 2L), sequences);
        assertEquals(0, ring.size());
    }

    @Test
    void fullBufferRefusesClaim() {
        send(1, 2, 3, 4);

        assertNull(ring.claim());       // producer must back off, not block
        assertEquals(4, ring.size());
    }

    @Test
    void drainingFreesSpace() {
        send(1, 2, 3, 4);
        ring.drain(collect, 1);         // frees one slot

        assertNotNull(ring.claim());
        ring.publish();
        assertNull(ring.claim());
    }

    @Test
    void maxLimitsTheBatch() {
        send(1, 2, 3);

        assertEquals(2, ring.drain(collect, 2));
        assertEquals(1, ring.drain(collect, 2));
        assertEquals(List.of(1L, 2L, 3L), values);
    }

    @Test
    void wrapsAroundManyTimes() {
        long next = 0;
        long expected = 0;
        for (int round = 0; round < 1_000; round++) {
            // Uneven batch sizes so head and tail land on every slot offset.
            int burst = 1 + round % 4;
            for (int i = 0; i < burst; i++) {
                Msg m = ring.claim();
                m.value = next++;
                ring.publish();
            }
            values.clear();
            ring.drain(collect, burst);
            for (long v : values) {
                assertEquals(expected++, v);
            }
        }
        assertEquals(next, expected);
    }

    @Test
    void slotsAreReusedNotReallocated() {
        Msg first = ring.claim();
        ring.publish();
        ring.drain(collect, 1);
        for (int i = 0; i < 3; i++) {
            ring.claim();
            ring.publish();
            ring.drain(collect, 1);
        }
        assertSame(first, ring.claim());    // back to slot 0, same object
    }

    @Test
    void publishWithoutClaimFails() {
        assertThrows(IllegalStateException.class, ring::publish);
    }

    @Test
    void claimTwiceWithoutPublishFails() {
        ring.claim();
        assertThrows(IllegalStateException.class, ring::claim);
    }

    @Test
    void handlerFailureRedeliversTheBatch() {
        send(1, 2);
        EventHandler<Msg> failing = (m, seq) -> {
            throw new IllegalStateException("boom");
        };
        assertThrows(IllegalStateException.class, () -> ring.drain(failing, 10));

        assertEquals(2, ring.drain(collect, 10));
        assertEquals(List.of(1L, 2L), values);
    }

    private void send(long... vs) {
        for (long v : vs) {
            Msg m = ring.claim();
            m.value = v;
            ring.publish();
        }
    }
}
