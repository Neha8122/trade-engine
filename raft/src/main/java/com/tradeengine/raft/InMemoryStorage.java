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
    private final List<LogEntry> log = new ArrayList<>();   // log.get(0) is index 1

    @Override public long currentTerm() { return currentTerm; }
    @Override public int votedFor() { return votedFor; }

    @Override
    public void saveTermAndVote(long term, int votedFor) {
        this.currentTerm = term;
        this.votedFor = votedFor;
    }

    @Override public long lastIndex() { return log.size(); }

    @Override
    public long termAt(long index) {
        return index == 0 ? 0 : log.get((int) index - 1).term();
    }

    @Override
    public LogEntry entry(long index) {
        return log.get((int) index - 1);
    }

    @Override
    public void append(List<LogEntry> entries) {
        log.addAll(entries);
    }

    @Override
    public void truncateFrom(long index) {
        log.subList((int) index - 1, log.size()).clear();
    }
}
