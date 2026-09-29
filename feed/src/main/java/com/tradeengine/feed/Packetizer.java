package com.tradeengine.feed;

import com.tradeengine.orderbook.Side;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Packs messages into numbered packets. Every message gets the next
 * sequence number; a packet's header holds the seqNo of its first message
 * and how many follow, so seqNos run continuously across packets.
 *
 * <p>A packet is sent when the next message wouldn't fit, or when the
 * owner calls {@link #flush()} at the end of an engine batch, so a message
 * never waits for more traffic before it goes out.
 */
public final class Packetizer {

    /** Fits in a standard 1500-byte Ethernet frame with IP and UDP headers. */
    public static final int DEFAULT_MAX_PACKET = 1400;

    private final ByteBuffer packet;
    private final int maxPacket;
    private final PacketSink sink;
    private final RetransmitStore store;    // may be null

    private long nextSeq = 1;
    private int count;
    private int position = Codec.HEADER_SIZE;
    private long packetsSent;

    public Packetizer(int maxPacket, PacketSink sink, RetransmitStore store) {
        if (maxPacket < Codec.HEADER_SIZE + Codec.MAX_MESSAGE_SIZE) {
            throw new IllegalArgumentException("maxPacket too small: " + maxPacket);
        }
        this.maxPacket = maxPacket;
        this.packet = ByteBuffer.allocateDirect(maxPacket).order(ByteOrder.LITTLE_ENDIAN);
        this.sink = sink;
        this.store = store;
    }

    void add(long orderId, Side side, long price, long qty) {
        int at = reserve(Codec.ADD_SIZE);
        committed(at, Codec.encodeAdd(packet, at, orderId, side, price, qty));
    }

    void execute(long orderId, long qty, long price) {
        int at = reserve(Codec.EXECUTE_SIZE);
        committed(at, Codec.encodeExecute(packet, at, orderId, qty, price));
    }

    void delete(long orderId) {
        int at = reserve(Codec.DELETE_SIZE);
        committed(at, Codec.encodeDelete(packet, at, orderId));
    }

    /** Sends the current packet if it has any messages. */
    public void flush() {
        if (count == 0) {
            return;
        }
        Codec.writeHeader(packet, nextSeq - count, count);
        packet.limit(position).position(0);
        sink.send(packet);
        packet.clear();
        packetsSent++;
        count = 0;
        position = Codec.HEADER_SIZE;
    }

    private int reserve(int size) {
        if (position + size > maxPacket) {
            flush();
        }
        return position;
    }

    private void committed(int at, int size) {
        if (store != null) {
            store.store(nextSeq, packet, at, size);
        }
        nextSeq++;
        count++;
        position += size;
    }

    /** SeqNo the next message will get. */
    public long nextSeq() { return nextSeq; }

    public long packetsSent() { return packetsSent; }
}
