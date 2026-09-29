package com.tradeengine.feed;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.tradeengine.orderbook.Side;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;

/** Real sockets: packets go through the OS network stack on localhost. */
class UdpLoopbackTest {

    @Test
    void packetsCrossARealSocket() throws Exception {
        try (UdpPacketSource source = new UdpPacketSource(0);
             UdpPacketSink sink = new UdpPacketSink(
                     new InetSocketAddress(InetAddress.getLoopbackAddress(), source.port()), null)) {

            Packetizer packetizer = new Packetizer(Packetizer.DEFAULT_MAX_PACKET, sink, null);
            MarketDataPublisher publisher = new MarketDataPublisher(packetizer);
            BookReplica replica = new BookReplica();
            FeedHandler handler = new FeedHandler(replica, null);

            publisher.onAdd(1, Side.BUY, 100_040, 30);
            publisher.onAdd(2, Side.SELL, 100_050, 20);
            publisher.flush();
            publisher.onExecute(2, 5, 100_050);
            publisher.onDelete(1);
            publisher.flush();

            long deadline = System.currentTimeMillis() + 5_000;
            while (handler.expected() < 5 && System.currentTimeMillis() < deadline) {
                if (!source.poll(handler)) {
                    Thread.sleep(1);
                }
            }

            assertEquals(5, handler.expected());
            assertEquals(2, handler.packets());
            assertEquals(0, replica.levelQty(Side.BUY, 100_040));
            assertEquals(15, replica.levelQty(Side.SELL, 100_050));
            assertEquals(1, replica.orderCount());
        }
    }
}
