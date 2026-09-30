package com.tradeengine.server;

import com.tradeengine.exchange.BookFactory;
import com.tradeengine.exchange.ExchangeNode;
import com.tradeengine.orderbook.ExecutionListener;
import com.tradeengine.orderbook.OrderType;
import com.tradeengine.orderbook.RejectReason;
import com.tradeengine.orderbook.Side;
import com.tradeengine.raft.RaftNode;
import com.tradeengine.raft.Role;
import com.tradeengine.raft.Storage;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@link NodeServer} that also takes client connections (docs/lld-gateway.html).
 *
 * <p>On the leader: logs clients on, runs pre-trade risk checks, submits
 * orders to Raft, and sends each client its acks, fills and cancels once the
 * order is committed and applied. On a follower: answers every order with
 * {@code NOT_LEADER} and the leader's id.
 *
 * <p>Order ownership (which client owns which order id) and the last trade
 * price are rebuilt from the committed log on every node, through the apply
 * hooks. So a follower that becomes leader already knows who owns what, and
 * risk checks keep working across a failover.
 */
public final class GatewayServer extends NodeServer {

    /** Risk limits. */
    public record Limits(long maxQty, long collarTicks, int maxOpenOrders,
                         double ordersPerSecond, int burst) {
        public static final Limits DEFAULT = new Limits(10_000, 50, 1_000, 10_000, 1_000);
    }

    private static final int MAX_CLIENT_FRAME = 1 << 16;
    private static final int MAX_CLIENT_BUFFER = 4 << 20;

    private record ClientOrder(int clientId, long clOrdId) { }

    /** One client TCP connection. */
    private static final class Session {
        final SocketChannel channel;
        final FrameReader in = new FrameReader(MAX_CLIENT_FRAME);
        final FrameWriter out = new FrameWriter(MAX_CLIENT_BUFFER);
        int clientId = -1;
        double tokens;
        long lastRefill = System.nanoTime();

        Session(SocketChannel channel, int burst) {
            this.channel = channel;
            this.tokens = burst;
        }
    }

    /** Lets the constructor hand the node a listener that points back at us. */
    private static final class Relay implements ExecutionListener, ExchangeNode.AckListener {
        GatewayServer target;
        @Override public void onAccepted(long orderId, long clOrdId) { target.onAccepted(orderId, clOrdId); }
        @Override public void onTrade(long maker, long taker, long price, long qty) { target.onTrade(maker, taker, price, qty); }
        @Override public void onCancelled(long orderId, long leaves) { target.onCancelled(orderId, leaves); }
        @Override public void onRejected(long clOrdId, RejectReason reason) { target.onRejected(clOrdId, reason); }
        @Override public void onAck(int clientId, long clOrdId) { target.onAck(clientId, clOrdId); }
    }

    private final Limits limits;
    private final ServerSocketChannel clientListener;
    private final Set<Session> sessions = new HashSet<>();
    private final Map<Integer, Session> byClient = new HashMap<>();

    // Rebuilt from the log on every node:
    private final Map<Long, ClientOrder> ownerOf = new HashMap<>();     // live orderId → its client order
    private final Map<ClientOrder, Long> orderIdOf = new HashMap<>();   // the reverse, for resend acks
    private final Map<Integer, Integer> openOrders = new HashMap<>();   // clientId → resting count
    private final List<Long> touched = new ArrayList<>();              // order ids this command touched
    private long lastTradePrice = -1;

    // Only for orders submitted through this node:
    private final Map<ClientOrder, Long> sendNanos = new HashMap<>();

    // The command being applied right now (between the apply hooks).
    private int applyingClient;
    private long applyingClOrdId;
    private boolean answered;

    public GatewayServer(int id, InetSocketAddress[] peers, InetSocketAddress clientAddress,
                         Storage storage, RaftNode.Config config, BookFactory books, Limits limits)
            throws IOException {
        this(id, peers, clientAddress, storage, config, books, limits, new Relay());
    }

    private GatewayServer(int id, InetSocketAddress[] peers, InetSocketAddress clientAddress,
                          Storage storage, RaftNode.Config config, BookFactory books, Limits limits,
                          Relay relay) throws IOException {
        super(id, peers, storage, config, books, relay, relay);
        relay.target = this;
        this.limits = limits;
        node.setApplyHooks(new ExchangeNode.ApplyHooks() {
            @Override
            public void before(int clientId, long clOrdId, boolean duplicate) {
                applyingClient = clientId;
                applyingClOrdId = clOrdId;
                answered = false;
            }

            @Override
            public void after() {
                // Orders that filled completely or were IOC leftovers are no
                // longer in the book: stop counting them as open.
                for (long id : touched) {
                    if (node.book().order(id) == null) {
                        leftBook(id);
                    }
                }
                touched.clear();
                applyingClient = -1;
            }
        });
        clientListener = ServerSocketChannel.open();
        clientListener.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        clientListener.bind(clientAddress);
        clientListener.configureBlocking(false);
        clientListener.register(selector, SelectionKey.OP_ACCEPT, "client-listener");
    }

    // --- connections ---

    @Override
    protected void acceptClient(SocketChannel ch) throws IOException {
        Session s = new Session(ch, limits.burst());
        sessions.add(s);
        ch.register(selector, SelectionKey.OP_READ, s);
    }

    @Override
    protected void handleOtherKey(SelectionKey key) throws IOException {
        if (!(key.attachment() instanceof Session s)) {
            return;
        }
        if (key.isReadable()) {
            if (s.in.readFrom(s.channel) < 0) {
                close(s);
                return;
            }
            s.in.drain((buf, at, len) -> onClientFrame(s, buf, at));
        }
        if (key.isValid() && key.isWritable()) {
            write(s);
        }
    }

    @Override
    protected void flushClients() throws IOException {
        for (Session s : Set.copyOf(sessions)) {
            if (s.out.pending() > 0) {
                write(s);
            }
        }
    }

    private void write(Session s) {
        try {
            boolean done = s.out.writeTo(s.channel);
            if (s.out.overLimit()) {
                close(s);                       // not reading: drop, don't buffer forever
                return;
            }
            SelectionKey key = s.channel.keyFor(selector);
            if (key != null && key.isValid()) {
                key.interestOps(done ? SelectionKey.OP_READ : SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            }
        } catch (IOException e) {
            close(s);
        }
    }

    @Override
    protected void onClientClosed(Object attachment) {
        if (attachment instanceof Session s) {
            forget(s);
        }
    }

    private void close(Session s) {
        try {
            s.channel.close();
        } catch (IOException ignored) {
            // already broken
        }
        forget(s);
    }

    private void forget(Session s) {
        sessions.remove(s);
        if (s.clientId != -1 && byClient.get(s.clientId) == s) {
            byClient.remove(s.clientId);
        }
    }

    // --- incoming client frames ---

    private void onClientFrame(Session s, ByteBuffer b, int at) {
        byte type = ClientProtocol.type(b, at);
        if (type == ClientProtocol.LOGON) {
            s.clientId = ClientProtocol.logonClientId(b, at);
            byClient.put(s.clientId, s);        // a reconnect replaces the old session
            return;
        }
        long clOrdId = ClientProtocol.clOrdId(b, at);
        if (s.clientId == -1) {
            ClientProtocol.reject(s.out, clOrdId, ClientProtocol.NOT_LOGGED_ON);
            return;
        }
        if (node.role() != Role.LEADER) {
            ClientProtocol.notLeader(s.out, node.leaderId());
            return;
        }
        if (!takeToken(s)) {
            ClientProtocol.reject(s.out, clOrdId, ClientProtocol.RISK_RATE_LIMIT);
            return;
        }
        if (type == ClientProtocol.NEW_ORDER) {
            onNewOrder(s, b, at, clOrdId);
        } else if (type == ClientProtocol.CANCEL) {
            long orderId = ClientProtocol.cancelOrderId(b, at);
            ClientOrder owner = ownerOf.get(orderId);
            if (owner == null || owner.clientId() != s.clientId) {
                // Unknown, finished, or someone else's: never cancel another client's order.
                ClientProtocol.reject(s.out, clOrdId, ClientProtocol.reasonCode(RejectReason.UNKNOWN_ORDER));
                return;
            }
            node.submitCancel(s.clientId, clOrdId, orderId);
        }
    }

    private void onNewOrder(Session s, ByteBuffer b, int at, long clOrdId) {
        long qty = ClientProtocol.qty(b, at);
        long price = ClientProtocol.price(b, at);
        OrderType type = ClientProtocol.orderType(b, at);
        int risk = riskCheck(s.clientId, type, price, qty);
        if (risk != 0) {
            // Refused before the log: costs no fsync, no replication.
            ClientProtocol.reject(s.out, clOrdId, risk);
            return;
        }
        ClientOrder key = new ClientOrder(s.clientId, clOrdId);
        sendNanos.put(key, ClientProtocol.sendNanos(b, at));
        node.submitNew(s.clientId, clOrdId, ClientProtocol.side(b, at), type, price, qty, System.nanoTime());
    }

    /** 0 if the order may go to the log, else a reject reason. */
    private int riskCheck(int clientId, OrderType type, long price, long qty) {
        if (qty > limits.maxQty()) {
            return ClientProtocol.RISK_MAX_QTY;
        }
        if (type != OrderType.MARKET && lastTradePrice != -1
                && Math.abs(price - lastTradePrice) > limits.collarTicks()) {
            return ClientProtocol.RISK_PRICE_COLLAR;    // fat finger
        }
        if (openOrders.getOrDefault(clientId, 0) >= limits.maxOpenOrders()) {
            return ClientProtocol.RISK_OPEN_ORDERS;
        }
        return 0;
    }

    /** Token bucket: refills at ordersPerSecond, holds at most burst. */
    private boolean takeToken(Session s) {
        long now = System.nanoTime();
        s.tokens = Math.min(limits.burst(), s.tokens + (now - s.lastRefill) * limits.ordersPerSecond() / 1e9);
        s.lastRefill = now;
        if (s.tokens < 1) {
            return false;
        }
        s.tokens -= 1;
        return true;
    }

    // --- book events, on every node, in log order ---

    private void onAccepted(long orderId, long clOrdId) {
        answered = true;
        ClientOrder key = new ClientOrder(applyingClient, clOrdId);
        ownerOf.put(orderId, key);
        orderIdOf.put(key, orderId);
        openOrders.merge(applyingClient, 1, Integer::sum);
        touched.add(orderId);
        Long sent = sendNanos.remove(key);
        Session s = byClient.get(applyingClient);
        if (s != null && sent != null) {
            ClientProtocol.ack(s.out, clOrdId, orderId, sent);
        }
    }

    private void onTrade(long maker, long taker, long price, long qty) {
        lastTradePrice = price;
        touched.add(maker);
        send(maker, price, qty);
        send(taker, price, qty);
    }

    private void send(long orderId, long price, long qty) {
        ClientOrder owner = ownerOf.get(orderId);
        Session s = owner == null ? null : byClient.get(owner.clientId());
        if (s != null) {
            ClientProtocol.fill(s.out, orderId, price, qty);
        }
    }

    private void onCancelled(long orderId, long leaves) {
        ClientOrder owner = ownerOf.get(orderId);
        Session s = owner == null ? null : byClient.get(owner.clientId());
        if (s != null) {
            ClientProtocol.cancelled(s.out, orderId, leaves);
        }
        leftBook(orderId);
    }

    private void onRejected(long clOrdId, RejectReason reason) {
        answered = true;
        sendNanos.remove(new ClientOrder(applyingClient, clOrdId));
        Session s = byClient.get(applyingClient);
        if (s != null) {
            ClientProtocol.reject(s.out, clOrdId, ClientProtocol.reasonCode(reason));
        }
    }

    /** A committed command this node proposed; covers resends that were deduplicated. */
    private void onAck(int clientId, long clOrdId) {
        if (answered) {
            return;                             // already acked or rejected above
        }
        // A resend of an order applied earlier: ack it again so the client
        // stops resending. orderId -1 means it has already left the book.
        ClientOrder key = new ClientOrder(clientId, clOrdId);
        Long sent = sendNanos.remove(key);
        Session s = byClient.get(clientId);
        if (s != null) {
            ClientProtocol.ack(s.out, clOrdId, orderIdOf.getOrDefault(key, -1L), sent == null ? 0 : sent);
        }
    }

    private void leftBook(long orderId) {
        ClientOrder owner = ownerOf.remove(orderId);
        if (owner != null) {
            orderIdOf.remove(owner);
            openOrders.merge(owner.clientId(), -1, Integer::sum);
        }
    }
}
