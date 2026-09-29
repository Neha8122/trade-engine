package com.tradeengine.orderbook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

class LongObjectMapTest {

    @Test
    void putGetRemove() {
        LongObjectMap<String> map = new LongObjectMap<>(8);
        assertNull(map.put(7, "seven"));
        assertEquals("seven", map.get(7));
        assertEquals(1, map.size());

        assertEquals("seven", map.remove(7));
        assertNull(map.get(7));
        assertEquals(0, map.size());
    }

    @Test
    void putReplacesAndReturnsOld() {
        LongObjectMap<String> map = new LongObjectMap<>(8);
        map.put(5, "a");
        assertEquals("a", map.put(5, "b"));
        assertEquals("b", map.get(5));
        assertEquals(1, map.size());
    }

    @Test
    void missingKeys() {
        LongObjectMap<String> map = new LongObjectMap<>(8);
        assertNull(map.get(99));
        assertNull(map.remove(99));
        assertEquals(0, map.size());
    }

    @Test
    void keyZeroIsReserved() {
        LongObjectMap<String> map = new LongObjectMap<>(8);
        assertThrows(IllegalArgumentException.class, () -> map.put(0, "x"));
    }

    @Test
    void fillsToMaxEntriesAndEmptiesAgain() {
        // Small table, so many keys share probe runs.
        int n = 16;
        LongObjectMap<Long> map = new LongObjectMap<>(n);
        for (long k = 1; k <= n; k++) {
            map.put(k, k * 10);
        }
        for (long k = 1; k <= n; k++) {
            assertEquals(k * 10, map.get(k));
        }
        // Remove odd keys: every removal shifts later entries back.
        for (long k = 1; k <= n; k += 2) {
            assertEquals(k * 10, map.remove(k));
        }
        for (long k = 1; k <= n; k++) {
            assertEquals(k % 2 == 0 ? Long.valueOf(k * 10) : null, map.get(k));
        }
        assertEquals(n / 2, map.size());
    }

    /**
     * The strong check: a long random mix of put/remove/get, compared at
     * every step against java.util.HashMap as the obviously-correct
     * reference. This is what catches mistakes in backward-shift deletion.
     */
    @Test
    void matchesHashMapUnderRandomOperations() {
        int maxLive = 64;
        LongObjectMap<Long> map = new LongObjectMap<>(maxLive);
        Map<Long, Long> ref = new HashMap<>();
        Random rnd = new Random(12345);     // fixed seed: failures reproduce

        for (int step = 0; step < 200_000; step++) {
            long key = 1 + rnd.nextInt(200);    // small key space → collisions
            int op = rnd.nextInt(3);
            if (op == 0 && (ref.size() < maxLive || ref.containsKey(key))) {
                long value = rnd.nextLong();
                assertEquals(ref.put(key, value), map.put(key, value), "put at step " + step);
            } else if (op == 1) {
                assertEquals(ref.remove(key), map.remove(key), "remove at step " + step);
            } else {
                assertEquals(ref.get(key), map.get(key), "get at step " + step);
            }
            assertEquals(ref.size(), map.size(), "size at step " + step);
        }
        for (Map.Entry<Long, Long> e : ref.entrySet()) {
            assertEquals(e.getValue(), map.get(e.getKey()));
        }
    }
}
