package com.tradeengine.ringbuffer;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.Supplier;

// Field layout, top to bottom (HotSpot lays out superclass fields first,
// so this class hierarchy fixes the order):
//
//   [56 B padding] [head, tailCache: consumer's line] [56 B padding]
//   [tail, headCache, claimed: producer's line] [56 B padding] [entries, mask]
//
// Each thread's hot fields sit on their own 64-byte cache line, so a write
// by one core never invalidates the line the other core is using
// (false sharing, docs/lld-ringbuffer.html diagram 2).

abstract class Pad0 {
    long p01, p02, p03, p04, p05, p06, p07;
}

abstract class ConsumerFields extends Pad0 {
    long head;          // written only by the consumer
    long tailCache;     // consumer's last view of tail
}

abstract class Pad1 extends ConsumerFields {
    long p11, p12, p13, p14, p15, p16, p17;
}

abstract class ProducerFields extends Pad1 {
    long tail;          // written only by the producer
    long headCache;     // producer's last view of head
    boolean claimed;    // a slot is claimed but not yet published
}

abstract class Pad2 extends ProducerFields {
    long p21, p22, p23, p24, p25, p26, p27;
}

/**
 * Lock-free queue for exactly one producer thread and one consumer thread.
 *
 * <p>Why no lock is needed: {@code tail} has one writer (the producer) and
 * {@code head} has one writer (the consumer), so nothing is ever written by
 * two threads. The only problem left is visibility, solved by publishing
 * each counter with a release write and reading the other side's counter
 * with an acquire read.
 *
 * <p>Slots are objects created once by the factory and reused forever: the
 * producer fills a slot's fields in place, so passing a message allocates
 * nothing.
 *
 * <p>Using it from more than one producer or more than one consumer thread
 * is a bug; nothing here detects it.
 */
public final class SpscRingBuffer<E> extends Pad2 {

    private static final VarHandle HEAD;
    private static final VarHandle TAIL;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            HEAD = lookup.findVarHandle(ConsumerFields.class, "head", long.class);
            TAIL = lookup.findVarHandle(ProducerFields.class, "tail", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Object[] entries;
    private final int mask;

    /**
     * @param capacity number of slots; must be a power of 2 so that
     *                 {@code sequence & mask} replaces a slow {@code %}
     * @param factory  creates every slot object, once, here
     */
    public SpscRingBuffer(int capacity, Supplier<E> factory) {
        if (capacity <= 0 || Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("capacity must be a power of 2: " + capacity);
        }
        entries = new Object[capacity];
        for (int i = 0; i < capacity; i++) {
            entries[i] = factory.get();
        }
        mask = capacity - 1;
    }

    // --- producer side ---

    /**
     * Returns the next slot to fill, or {@code null} if the buffer is full.
     * Fill its fields, then call {@link #publish()}. Producer thread only.
     */
    @SuppressWarnings("unchecked")
    public E claim() {
        if (claimed) {
            throw new IllegalStateException("previous claim not published");
        }
        long t = tail;
        if (t - headCache >= entries.length) {
            // Looks full. Only now look at the consumer's counter: reading
            // it pulls the consumer's cache line over, so do it rarely.
            headCache = (long) HEAD.getAcquire(this);
            if (t - headCache >= entries.length) {
                return null;
            }
        }
        claimed = true;
        return (E) entries[(int) (t & mask)];
    }

    /**
     * Makes the claimed slot visible to the consumer. The release write means
     * every field written into the slot before this call is visible to the
     * consumer once it sees the new tail.
     */
    public void publish() {
        if (!claimed) {
            throw new IllegalStateException("publish without claim");
        }
        claimed = false;
        TAIL.setRelease(this, tail + 1);
    }

    // --- consumer side ---

    /**
     * Hands up to {@code max} waiting messages to {@code handler}, oldest
     * first, then frees their slots with one write. Returns how many were
     * handled; 0 means the buffer was empty. Consumer thread only.
     *
     * <p>If the handler throws, none of this batch is freed, so those
     * messages are delivered again by the next call.
     */
    @SuppressWarnings("unchecked")
    public int drain(EventHandler<E> handler, int max) {
        long h = head;
        long available = tailCache - h;
        if (available == 0) {
            // Looks empty. Only now look at the producer's counter.
            tailCache = (long) TAIL.getAcquire(this);
            available = tailCache - h;
            if (available == 0) {
                return 0;
            }
        }
        int n = (int) Math.min(available, max);
        for (int i = 0; i < n; i++) {
            long seq = h + i;
            handler.onEvent((E) entries[(int) (seq & mask)], seq);
        }
        // One release write frees the whole batch, instead of one per message.
        HEAD.setRelease(this, h + n);
        return n;
    }

    // --- monitoring (any thread, approximate) ---

    public int capacity() {
        return entries.length;
    }

    /** Messages waiting right now. Only a snapshot: both sides keep moving. */
    public long size() {
        long h = (long) HEAD.getAcquire(this);
        long t = (long) TAIL.getAcquire(this);
        return t - h;
    }
}
