package com.tradeengine.orderbook;

/**
 * {@code long → V} hash map with no boxing and no allocation after startup.
 * Used as the {@code orderId → Order} index so cancel is O(1).
 *
 * <p>Why not {@code HashMap<Long, Order>}: every put boxes the key into a
 * {@code Long} and allocates an entry node, which is garbage per order.
 *
 * <p>Open addressing with linear probing: keys and values live in two
 * flat arrays, so a lookup is usually one or two adjacent memory reads.
 * Key {@code 0} marks an empty slot, so 0 is not a valid key (order ids
 * start at 1).
 *
 * <p>Fixed size, never resized mid-session (a resize would allocate and
 * stall). Size it for the most live entries you allow; the table is made
 * at least twice that, so probes stay short.
 */
public final class LongObjectMap<V> {

    private static final long GOLDEN = 0x9E3779B97F4A7C15L;

    private final long[] keys;
    private final Object[] values;
    private final int mask;
    private final int shift;
    private int size;

    public LongObjectMap(int maxEntries) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be > 0");
        }
        int capacity = Integer.highestOneBit(maxEntries * 2 - 1) << 1;
        keys = new long[capacity];
        values = new Object[capacity];
        mask = capacity - 1;
        shift = 64 - Integer.numberOfTrailingZeros(capacity);
    }

    /**
     * Fibonacci hashing: multiply by 2^64/φ and keep the top bits. Spreads
     * sequential ids (1, 2, 3, ...) across the table instead of clustering.
     */
    private int home(long key) {
        return (int) ((key * GOLDEN) >>> shift) & mask;
    }

    @SuppressWarnings("unchecked")
    public V get(long key) {
        for (int i = home(key); keys[i] != 0; i = (i + 1) & mask) {
            if (keys[i] == key) {
                return (V) values[i];
            }
        }
        return null;
    }

    /** Inserts or replaces. Returns the previous value, or {@code null}. */
    @SuppressWarnings("unchecked")
    public V put(long key, V value) {
        if (key == 0) {
            throw new IllegalArgumentException("key 0 is reserved for empty");
        }
        int i = home(key);
        while (keys[i] != 0) {
            if (keys[i] == key) {
                V old = (V) values[i];
                values[i] = value;
                return old;
            }
            i = (i + 1) & mask;
        }
        if (size == mask) {
            // Keep one slot empty so probe loops always terminate.
            throw new IllegalStateException("map full: " + size);
        }
        keys[i] = key;
        values[i] = value;
        size++;
        return null;
    }

    /**
     * Removes and returns the value, or {@code null} if absent.
     *
     * <p>Uses backward-shift deletion instead of tombstones: after emptying
     * a slot, later entries in the same probe run are moved back into it
     * if their home slot allows. The table never fills up with dead slots,
     * so lookups stay fast however long the session runs.
     */
    @SuppressWarnings("unchecked")
    public V remove(long key) {
        int i = home(key);
        while (keys[i] != key) {
            if (keys[i] == 0) {
                return null;
            }
            i = (i + 1) & mask;
        }
        V old = (V) values[i];

        int j = i;
        while (true) {
            j = (j + 1) & mask;
            if (keys[j] == 0) {
                break;
            }
            int k = home(keys[j]);
            // The entry at j may move to the hole at i only if its home k
            // is NOT cyclically inside (i, j]; otherwise moving it would put
            // it before its home, and get() would never find it.
            boolean staysPut = (i <= j) ? (i < k && k <= j) : (i < k || k <= j);
            if (!staysPut) {
                keys[i] = keys[j];
                values[i] = values[j];
                i = j;
            }
        }
        keys[i] = 0;
        values[i] = null;
        size--;
        return old;
    }

    public int size() { return size; }
}
