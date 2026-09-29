package com.tradeengine.raft;

/** Delivers messages to other nodes: a simulated network in tests, TCP later. */
@FunctionalInterface
public interface Transport {

    /** Fire and forget: the message may be lost, delayed or reordered. */
    void send(Message message);
}
