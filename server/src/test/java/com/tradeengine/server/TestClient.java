package com.tradeengine.server;

import com.tradeengine.orderbook.OrderType;
import com.tradeengine.orderbook.Side;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/** A simple blocking client: sends frames, collects replies as readable strings. */
final class TestClient implements AutoCloseable {

    private final SocketChannel channel;
    private final FrameReader in = new FrameReader(1 << 16);
    private final FrameWriter out = new FrameWriter(1 << 20);
    final List<String> received = new ArrayList<>();

    TestClient(InetSocketAddress server) throws IOException {
        channel = SocketChannel.open(server);
        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        channel.configureBlocking(false);
    }

    TestClient logon(int clientId) throws IOException {
        ClientProtocol.logon(out, clientId);
        return flush();
    }

    TestClient order(long clOrdId, Side side, OrderType type, long price, long qty) throws IOException {
        ClientProtocol.newOrder(out, clOrdId, side, type, price, qty, System.nanoTime());
        return flush();
    }

    TestClient cancel(long clOrdId, long orderId) throws IOException {
        ClientProtocol.cancel(out, clOrdId, orderId);
        return flush();
    }

    private TestClient flush() throws IOException {
        while (!out.writeTo(channel)) {
            Thread.onSpinWait();
        }
        return this;
    }

    /** Reads until some received line matches, or fails after 10 s. */
    String await(Predicate<String> wanted, String what) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (true) {
            for (String line : received) {
                if (wanted.test(line)) {
                    return line;
                }
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("never received " + what + "; got " + received);
            }
            if (in.readFrom(channel) < 0) {
                throw new AssertionError("server closed the connection; got " + received);
            }
            in.drain((b, at, len) -> received.add(describe(b, at)));
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    }

    String await(String prefix) throws IOException {
        return await(l -> l.startsWith(prefix), prefix);
    }

    /** Order id from an ACK line like "ACK clOrd=5 order=12". */
    static long orderId(String ackLine) {
        return Long.parseLong(ackLine.substring(ackLine.indexOf("order=") + 6));
    }

    static String describe(ByteBuffer b, int at) {
        return switch (ClientProtocol.type(b, at)) {
            case ClientProtocol.ACK -> "ACK clOrd=" + ClientProtocol.clOrdId(b, at)
                    + " order=" + ClientProtocol.ackOrderId(b, at);
            case ClientProtocol.FILL -> "FILL order=" + ClientProtocol.fillOrderId(b, at)
                    + " " + ClientProtocol.fillQty(b, at) + "@" + ClientProtocol.fillPrice(b, at);
            case ClientProtocol.CANCELLED -> "CANCELLED order=" + ClientProtocol.cancelledOrderId(b, at)
                    + " leaves=" + ClientProtocol.cancelledLeaves(b, at);
            case ClientProtocol.REJECT -> "REJECT clOrd=" + ClientProtocol.clOrdId(b, at)
                    + " reason=" + ClientProtocol.rejectReason(b, at);
            case ClientProtocol.NOT_LEADER -> "NOT_LEADER " + ClientProtocol.notLeaderId(b, at);
            default -> "UNKNOWN " + ClientProtocol.type(b, at);
        };
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
