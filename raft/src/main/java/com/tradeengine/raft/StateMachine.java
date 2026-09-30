package com.tradeengine.raft;

/**
 * What the log drives. Called with committed entries only, in log order,
 * exactly once each per node lifetime. For the exchange this is the
 * OrderBook: same entries in the same order give the same book on every
 * node.
 */
@FunctionalInterface
public interface StateMachine {

    /** {@code command} is never a no-op; those are skipped. */
    void apply(long index, byte[] command);

    /**
     * The whole state as bytes, reflecting every entry applied so far.
     * Only needed when snapshots are switched on ({@code Config.snapshotEvery}).
     */
    default byte[] snapshot() {
        throw new UnsupportedOperationException("this state machine can't snapshot");
    }

    /** Replaces the whole state with one produced by {@link #snapshot()}. */
    default void restore(byte[] snapshot) {
        throw new UnsupportedOperationException("this state machine can't restore");
    }
}
