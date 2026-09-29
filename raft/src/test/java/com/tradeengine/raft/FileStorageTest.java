package com.tradeengine.raft;

import static com.tradeengine.raft.StorageContractTest.entry;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** What survives a restart, and what happens when a crash damaged a write. */
class FileStorageTest {

    @TempDir
    Path dir;

    @Test
    void everythingSurvivesReopen() throws Exception {
        try (FileStorage s = new FileStorage(dir)) {
            s.saveTermAndVote(7, 1);
            s.append(List.of(entry(6, 1), entry(7, 2), new LogEntry(7, LogEntry.NO_OP)));
        }
        try (FileStorage s = new FileStorage(dir)) {
            assertEquals(7, s.currentTerm());
            assertEquals(1, s.votedFor());
            assertEquals(3, s.lastIndex());
            assertEquals(6, s.termAt(1));
            assertArrayEquals(new byte[] {2}, s.entry(2).command());
            assertEquals(0, s.entry(3).command().length);
        }
    }

    @Test
    void truncationSurvivesReopen() throws Exception {
        try (FileStorage s = new FileStorage(dir)) {
            s.append(List.of(entry(1, 1), entry(1, 2), entry(1, 3)));
            s.truncateFrom(2);
            s.append(List.of(entry(2, 8)));
        }
        try (FileStorage s = new FileStorage(dir)) {
            assertEquals(2, s.lastIndex());
            assertEquals(2, s.termAt(2));
            assertArrayEquals(new byte[] {8}, s.entry(2).command());
        }
    }

    @Test
    void halfWrittenLastRecordIsDropped() throws Exception {
        try (FileStorage s = new FileStorage(dir)) {
            s.append(List.of(entry(1, 1), entry(1, 2)));
        }
        // A crash in the middle of writing a third record: only part of it hit disk.
        Files.write(dir.resolve("log"), new byte[] {13, 0, 0, 0, 1, 2}, StandardOpenOption.APPEND);

        try (FileStorage s = new FileStorage(dir)) {
            assertEquals(2, s.lastIndex());
            s.append(List.of(entry(1, 3)));     // appends cleanly after the cut
        }
        try (FileStorage s = new FileStorage(dir)) {
            assertEquals(3, s.lastIndex());
            assertArrayEquals(new byte[] {3}, s.entry(3).command());
        }
    }

    @Test
    void recordWithBadChecksumAndEverythingAfterIsDropped() throws Exception {
        try (FileStorage s = new FileStorage(dir)) {
            s.append(List.of(entry(1, 1), entry(1, 2), entry(1, 3)));
        }
        // Each record is 8 (header) + 8 (term) + 1 (command) = 17 bytes.
        // Flip one byte inside record 2's command.
        flipByte(dir.resolve("log"), 17 + 16);

        try (FileStorage s = new FileStorage(dir)) {
            assertEquals(1, s.lastIndex());
            assertEquals(17, Files.size(dir.resolve("log")), "file cut after the last good record");
        }
    }

    @Test
    void damagedLatestMetaSlotFallsBackToPreviousTermAndVote() throws Exception {
        try (FileStorage s = new FileStorage(dir)) {
            s.saveTermAndVote(4, 2);            // version 1 → slot 1
            s.saveTermAndVote(5, 0);            // version 2 → slot 0
        }
        // A crash tore the write of slot 0 (the newer one).
        flipByte(dir.resolve("meta"), 10);

        try (FileStorage s = new FileStorage(dir)) {
            assertEquals(4, s.currentTerm());
            assertEquals(2, s.votedFor());
        }
    }

    @Test
    void nodeRestartedFromDiskRefusesASecondVoteInTheSameTerm() throws Exception {
        java.util.List<Message> sent = new java.util.ArrayList<>();
        try (FileStorage disk = new FileStorage(dir)) {
            RaftNode node = new RaftNode(0, 3, disk, sent::add, (i, c) -> { },
                    new java.util.Random(1), RaftNode.Config.DEFAULT);
            node.handle(new Message.RequestVote(1, 0, 5, 0, 0));
        }                                       // process dies right after voting

        try (FileStorage disk = new FileStorage(dir)) {
            RaftNode node = new RaftNode(0, 3, disk, sent::add, (i, c) -> { },
                    new java.util.Random(2), RaftNode.Config.DEFAULT);
            node.handle(new Message.RequestVote(2, 0, 5, 0, 0));
        }

        assertEquals(new Message.VoteResponse(0, 1, 5, true), sent.get(0));
        assertEquals(new Message.VoteResponse(0, 2, 5, false), sent.get(1),
                "after a restart the node must remember it already voted in term 5");
    }

    private static void flipByte(Path file, long at) throws Exception {
        try (RandomAccessFile f = new RandomAccessFile(file.toFile(), "rw")) {
            f.seek(at);
            int b = f.read();
            f.seek(at);
            f.write(b ^ 0xFF);
        }
    }
}
