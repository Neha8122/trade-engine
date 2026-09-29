package com.tradeengine.raft;

import java.util.List;

/**
 * Everything a node must remember across a crash: its term, who it voted
 * for in that term, and its log. Every method that changes state must be
 * durable (fsynced) before it returns, because the node replies to other
 * nodes right after calling it. A node that forgot its vote could vote
 * twice in one term and let two leaders be elected.
 *
 * <p>Log indexes start at 1; index 0 means "before the first entry" and
 * has term 0.
 */
public interface Storage {

    long currentTerm();

    /** Node id voted for in {@link #currentTerm()}, or -1. */
    int votedFor();

    void saveTermAndVote(long term, int votedFor);

    long lastIndex();

    /** Term of the entry at {@code index}; 0 for index 0. */
    long termAt(long index);

    LogEntry entry(long index);

    /** Appends after the current last entry. */
    void append(List<LogEntry> entries);

    /** Deletes the entry at {@code index} and everything after it. */
    void truncateFrom(long index);
}
