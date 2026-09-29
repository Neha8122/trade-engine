package com.tradeengine.feed;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

/**
 * Sends packets as UDP datagrams. Pointed at a multicast group (e.g.
 * 239.1.1.1:5000) one send reaches every subscriber that joined it; pointed
 * at a normal address it is plain unicast, which the tests use on localhost.
 */
public final class UdpPacketSink implements PacketSink, AutoCloseable {

    private final DatagramChannel channel;
    private final InetSocketAddress target;
    private long sendFailures;

    /** @param multicastInterface interface to send multicast on, or null for default */
    public UdpPacketSink(InetSocketAddress target, NetworkInterface multicastInterface) throws IOException {
        this.target = target;
        this.channel = DatagramChannel.open(StandardProtocolFamily.INET);
        if (multicastInterface != null) {
            channel.setOption(StandardSocketOptions.IP_MULTICAST_IF, multicastInterface);
        }
    }

    @Override
    public void send(ByteBuffer packet) {
        try {
            if (channel.send(packet, target) == 0) {
                // Socket buffer full: UDP drops it, like any loss on the
                // network. Subscribers see a gap and recover.
                sendFailures++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public long sendFailures() { return sendFailures; }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
