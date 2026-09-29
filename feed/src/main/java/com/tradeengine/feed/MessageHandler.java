package com.tradeengine.feed;

import java.nio.ByteBuffer;

/** Receives one encoded message: the bytes at {@code offset} in {@code buf}. */
@FunctionalInterface
public interface MessageHandler {

    void onMessage(ByteBuffer buf, int offset, long seqNo);
}
