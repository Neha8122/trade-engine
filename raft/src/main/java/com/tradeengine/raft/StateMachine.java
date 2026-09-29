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
}
