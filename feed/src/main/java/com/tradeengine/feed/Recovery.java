package com.tradeengine.feed;

/** Source of messages a subscriber missed. */
@FunctionalInterface
public interface Recovery {

    /**
     * Delivers messages {@code from} to {@code toInclusive}, in order.
     *
     * @return false if any of them is no longer available (too old); then
     *         nothing is delivered and the subscriber needs a snapshot
     */
    boolean replay(long from, long toInclusive, MessageHandler handler);
}
