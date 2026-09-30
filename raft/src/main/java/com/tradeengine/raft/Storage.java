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
 * has term 0. After a snapshot, entries up to {@link #snapshotIndex()} are
 * gone: {@link #termAt} still answers for the snapshot index itself, but
 * nothing below it can be read.
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

    /** Last log index covered by the snapshot, or 0 if there is none. */
    long snapshotIndex();

    /** Term of the entry at {@link #snapshotIndex()}. */
    long snapshotTerm();

    /** The state machine's bytes at {@link #snapshotIndex()}, or null if none. */
    byte[] snapshotData();

    /**
     * Stores a snapshot of the state at {@code index} and drops the log up to
     * it, durably. If the log holds the entry at {@code index} with
     * {@code term}, the entries after it are kept (a node compacting its own
     * log); otherwise the whole log is discarded (a follower receiving a
     * leader's snapshot that its log disagrees with).
     */
    void installSnapshot(long index, long term, byte[] data);

    /**
     * For storage in group-commit mode: makes every log change since the
     * last call durable. The caller must not send anything that depends on
     * those changes before this returns. Storage that syncs on every write
     * does nothing here.
     */
    default void sync() { }

    /**
     * Asks for everything written so far to become durable, without waiting.
     * Returns a token to pass to {@link #isDurable}. Storage without a
     * background sync thread does the work right here and returns a token
     * that is already durable.
     */
    default long requestSync() {
        sync();
        return 0;
    }

    /** True once every change made before {@code requestSync()} returned {@code token} is on disk. */
    default boolean isDurable(long token) {
        return true;
    }

    /** Called on the sync thread each time more becomes durable (e.g. to wake an event loop). */
    default void onDurable(Runnable callback) { }
}
