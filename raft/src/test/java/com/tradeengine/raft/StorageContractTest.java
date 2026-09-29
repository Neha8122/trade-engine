package com.tradeengine.raft;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Both storages must behave identically; the simulator relies on that. */
class StorageContractTest {

    @TempDir
    Path dir;

    static Stream<Arguments> storages() {
        return Stream.of(
                Arguments.of("memory", (Function<Path, Storage>) d -> new InMemoryStorage()),
                Arguments.of("file", (Function<Path, Storage>) FileStorage::new));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("storages")
    void startsEmpty(String name, Function<Path, Storage> open) {
        Storage s = open.apply(dir);
        assertEquals(0, s.currentTerm());
        assertEquals(-1, s.votedFor());
        assertEquals(0, s.lastIndex());
        assertEquals(0, s.termAt(0));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("storages")
    void termAndVote(String name, Function<Path, Storage> open) {
        Storage s = open.apply(dir);
        s.saveTermAndVote(3, 2);
        s.saveTermAndVote(4, -1);
        assertEquals(4, s.currentTerm());
        assertEquals(-1, s.votedFor());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("storages")
    void appendAndRead(String name, Function<Path, Storage> open) {
        Storage s = open.apply(dir);
        s.append(List.of(entry(1, 10), entry(1, 11)));
        s.append(List.of(entry(2, 12)));

        assertEquals(3, s.lastIndex());
        assertEquals(1, s.termAt(2));
        assertEquals(2, s.termAt(3));
        assertArrayEquals(new byte[] {12}, s.entry(3).command());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("storages")
    void truncateThenAppend(String name, Function<Path, Storage> open) {
        Storage s = open.apply(dir);
        s.append(List.of(entry(1, 1), entry(1, 2), entry(1, 3), entry(1, 4)));
        s.truncateFrom(3);
        s.append(List.of(entry(2, 9)));

        assertEquals(3, s.lastIndex());
        assertEquals(2, s.termAt(3));
        assertArrayEquals(new byte[] {9}, s.entry(3).command());
        assertArrayEquals(new byte[] {2}, s.entry(2).command());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("storages")
    void noOpEntriesSurvive(String name, Function<Path, Storage> open) {
        Storage s = open.apply(dir);
        s.append(List.of(new LogEntry(5, LogEntry.NO_OP)));
        assertEquals(5, s.termAt(1));
        assertEquals(0, s.entry(1).command().length);
    }

    static LogEntry entry(long term, int value) {
        return new LogEntry(term, new byte[] {(byte) value});
    }
}
