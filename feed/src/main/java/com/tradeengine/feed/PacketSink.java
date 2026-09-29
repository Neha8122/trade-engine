package com.tradeengine.feed;

import java.nio.ByteBuffer;

/**
 * Where finished packets go: UDP multicast in production, a fake network
 * in tests. The buffer is reused after {@code send} returns, so a sink that
 * keeps the packet must copy it.
 */
@FunctionalInterface
public interface PacketSink {

    /** {@code packet} holds one packet from position to limit. */
    void send(ByteBuffer packet);
}
