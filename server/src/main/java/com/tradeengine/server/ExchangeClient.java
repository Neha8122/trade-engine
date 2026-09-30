package com.tradeengine.server;

import com.tradeengine.orderbook.OrderType;
import com.tradeengine.orderbook.Side;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A client connection that survives failover. Not thread-safe: one thread
 * calls {@link #send} and {@link #poll} in a loop.
 *
 * <ul>
 *   <li>Every order stays in {@code unacked} until the exchange acks or
 *       rejects it.</li>
 *   <li>{@code NOT_LEADER n}: reconnect to node n. A broken connection:
 *       try the next node.</li>
 *   <li>After every (re)connect: log on, then resend everything still
 *       unacked with the same clOrdIds. The exchange drops duplicates, so a
 *       resend can never create a second order.</li>
 * </ul>
 *
 * Latency is measured from the order's <b>intended</b> send time, which
 * the caller supplies. If the exchange stalls, orders that should have gone
 * out during the stall count as late instead of silently not being sent
 * (no "coordinated omission"), and time spent in failover counts too.
 */
public final class ExchangeClient implements AutoCloseable {

    public interface Listener {
        void onAck(long clOrdId, long orderId, long latencyNanos);
        void onReject(long clOrdId, int reason);
        default void onFill(long orderId, long price, long qty) { }
        default void onConnected(int node) { }
    }

    private record Pending(Side side, OrderType type, long price, long qty, long intendedNanos) { }

    private static final long RETRY_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

    private final InetSocketAddress[] nodes;
    private final int clientId;
    private final Listener listener;
    private final Map<Long, Pending> unacked = new LinkedHashMap<>();

    private int current;
    private SocketChannel channel;
    private boolean connected;
    private long retryAt;
    private FrameReader in;
    private FrameWriter out;
    private int reconnects = -1;

    public ExchangeClient(InetSocketAddress[] nodes, int clientId, Listener listener) {
        this.nodes = nodes;
        this.clientId = clientId;
        this.listener = listener;
    }

    /** Queues an order; it is sent now if connected, else after the next connect. */
    public void send(long clOrdId, Side side, OrderType type, long price, long qty, long intendedNanos) {
        Pending p = new Pending(side, type, price, qty, intendedNanos);
        unacked.put(clOrdId, p);
        if (connected) {
            write(clOrdId, p);
        }
    }

    /** Connects if needed, reads replies, writes pending bytes. Never blocks. */
    public void poll() {
        try {
            if (channel == null) {
                if (System.nanoTime() >= retryAt) {
                    connect();
                }
                return;
            }
            if (!connected) {
                if (channel.finishConnect()) {
                    onConnected();
                }
                return;
            }
            if (in.readFrom(channel) < 0) {
                fail();
                return;
            }
            in.drain(this::onFrame);
            if (channel != null && out.pending() > 0) {
                out.writeTo(channel);
            }
        } catch (IOException e) {
            fail();
        }
    }

    private void connect() throws IOException {
        channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        connected = false;
        if (channel.connect(nodes[current])) {
            onConnected();
        }
    }

    private void onConnected() throws IOException {
        connected = true;
        reconnects++;
        in = new FrameReader(1 << 16);
        out = new FrameWriter(Integer.MAX_VALUE);
        ClientProtocol.logon(out, clientId);
        for (Map.Entry<Long, Pending> e : unacked.entrySet()) {
            write(e.getKey(), e.getValue());    // resend, same clOrdIds
        }
        out.writeTo(channel);
        listener.onConnected(current);
    }

    private void write(long clOrdId, Pending p) {
        ClientProtocol.newOrder(out, clOrdId, p.side, p.type, p.price, p.qty, p.intendedNanos);
    }

    private void onFrame(ByteBuffer b, int at, int len) {
        switch (ClientProtocol.type(b, at)) {
            case ClientProtocol.ACK -> {
                long clOrdId = ClientProtocol.clOrdId(b, at);
                Pending p = unacked.remove(clOrdId);
                if (p != null) {
                    listener.onAck(clOrdId, ClientProtocol.ackOrderId(b, at), System.nanoTime() - p.intendedNanos);
                }
            }
            case ClientProtocol.REJECT -> {
                long clOrdId = ClientProtocol.clOrdId(b, at);
                if (unacked.remove(clOrdId) != null) {
                    listener.onReject(clOrdId, ClientProtocol.rejectReason(b, at));
                }
            }
            case ClientProtocol.FILL -> listener.onFill(ClientProtocol.fillOrderId(b, at),
                    ClientProtocol.fillPrice(b, at), ClientProtocol.fillQty(b, at));
            case ClientProtocol.NOT_LEADER -> {
                int leader = ClientProtocol.notLeaderId(b, at);
                switchTo(leader >= 0 ? leader : (current + 1) % nodes.length, leader >= 0 ? 0 : RETRY_NANOS);
            }
            default -> { }
        }
    }

    private void fail() {
        switchTo((current + 1) % nodes.length, RETRY_NANOS);
    }

    private void switchTo(int node, long delayNanos) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
                // already broken
            }
        }
        channel = null;
        connected = false;
        current = node;
        retryAt = System.nanoTime() + delayNanos;
    }

    public int unacked() { return unacked.size(); }
    public int reconnects() { return Math.max(0, reconnects); }
    public boolean isConnected() { return connected; }

    @Override
    public void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
    }
}
