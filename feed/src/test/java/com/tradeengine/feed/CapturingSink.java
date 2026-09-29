package com.tradeengine.feed;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/** Test sink: keeps a copy of every packet (the packetizer reuses its buffer). */
final class CapturingSink implements PacketSink {

    final List<ByteBuffer> packets = new ArrayList<>();

    @Override
    public void send(ByteBuffer packet) {
        packets.add(copy(packet));
    }

    static ByteBuffer copy(ByteBuffer packet) {
        ByteBuffer c = ByteBuffer.allocate(packet.remaining()).order(ByteOrder.LITTLE_ENDIAN);
        c.put(packet.duplicate()).flip();
        return c;
    }
}
