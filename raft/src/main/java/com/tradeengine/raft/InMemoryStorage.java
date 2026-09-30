package com.tradeengine.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * Storage kept in memory. In the simulator a "crash" throws away the
 * RaftNode but keeps this object, which models a disk that survived.
 */
public final class InMemoryStorage implements Storage {

    private long currentTerm;
    private int votedFor = -1;
    private final List<LogEntry> log = new ArrayList<>();   // log.get(0) is snapshotIndex + 1
    private long snapshotIndex;
    private long snapshotTerm;
    private byte[] snapshotData;

    @Override public long currentTerm() { return currentTerm; }
    @Override public int votedFor() { return votedFor; }

    @Override
    public void saveTermAndVote(long term, int votedFor) {
        this.currentTerm = term;
        this.votedFor = votedFor;
    }

    @Override public long lastIndex() { return snapshotIndex + log.size(); }

    @Override
    public long termAt(long index) {
        if (index == snapshotIndex) {
            return snapshotTerm;                // 0 for index 0 with no snapshot
        }
        return entry(index).term();
    }

    @Override
    public LogEntry entry(long index) {
        if (index <= snapshotIndex) {
            throw new IllegalArgumentException("index " + index + " is compacted into the snapshot");
        }
        return log.get((int) (index - snapshotIndex - 1));
    }

    @Override
    public void append(List<LogEntry> entries) {
        log.addAll(entries);
    }

    @Override
    public void truncateFrom(long index) {
        log.subList((int) (index - snapshotIndex - 1), log.size()).clear();
    }

    @Override public long snapshotIndex() { return snapshotIndex; }
    @Override public long snapshotTerm() { return snapshotTerm; }
    @Override public byte[] snapshotData() { return snapshotData; }

    @Override
    public void installSnapshot(long index, long term, byte[] data) {
        if (index <= snapshotIndex) {
            return;                             // we already have a newer one
        }
        boolean keepSuffix = index > snapshotIndex && index <= lastIndex() && termAt(index) == term;
        List<LogEntry> suffix = keepSuffix
                ? new ArrayList<>(log.subList((int) (index - snapshotIndex), log.size()))
                : List.of();
        log.clear();
        log.addAll(suffix);
        snapshotIndex = index;
        snapshotTerm = term;
        snapshotData = data;
    }
}
