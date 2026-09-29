package com.tradeengine.feed;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.DatagramChannel;

/**
 * Subscriber's socket. Non-blocking: {@link #poll} returns at once, so the
 * subscriber loop can spin on it like the engine spins on its ring buffer.
 */
public final class UdpPacketSource implements AutoCloseable {

    private final DatagramChannel channel;
    private final ByteBuffer buffer =
            ByteBuffer.allocateDirect(Packetizer.DEFAULT_MAX_PACKET).order(ByteOrder.LITTLE_ENDIAN);

    /** Binds to {@code port} on all interfaces; 0 picks a free port. */
    public UdpPacketSource(int port) throws IOException {
        channel = DatagramChannel.open(StandardProtocolFamily.INET);
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        channel.bind(new InetSocketAddress(port));
        channel.configureBlocking(false);
    }

    /** Joins a multicast group so this socket receives what is sent to it. */
    public void join(InetAddress group, NetworkInterface nic) throws IOException {
        channel.join(group, nic);
    }

    public int port() throws IOException {
        return ((InetSocketAddress) channel.getLocalAddress()).getPort();
    }

    /**
     * Receives at most one packet and hands it to {@code handler}.
     * Returns false if nothing was waiting.
     */
    public boolean poll(FeedHandler handler) throws IOException {
        buffer.clear();
        SocketAddress from = channel.receive(buffer);
        if (from == null) {
            return false;
        }
        buffer.flip();
        handler.onPacket(buffer);
        return true;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
