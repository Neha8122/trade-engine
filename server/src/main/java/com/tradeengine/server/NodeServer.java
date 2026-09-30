package com.tradeengine.server;

import com.tradeengine.exchange.BookFactory;
import com.tradeengine.exchange.ExchangeNode;
import com.tradeengine.orderbook.ExecutionListener;
import com.tradeengine.raft.Message;
import com.tradeengine.raft.RaftNode;
import com.tradeengine.raft.Role;
import com.tradeengine.raft.Storage;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * One exchange server process (docs/lld-gateway.html §2). A single thread
 * runs the event loop and owns everything: the sockets, the Raft node, the
 * order book and the log. Nothing is shared, so nothing is locked.
 *
 * <p>Each turn of the loop: wait for network data or the next tick, read
 * and handle every frame that arrived, tick Raft every 10 ms, propose
 * whatever orders arrived this turn as one batch, and write everything out.
 *
 * <p>Raft traffic: this node keeps one outgoing connection to every peer
 * for sending (retried if it breaks) and accepts one incoming connection
 * from every peer for receiving. Raft tolerates lost messages, so a
 * message for a peer that is currently unreachable is simply dropped.
 */
public class NodeServer implements AutoCloseable {

    private static final long TICK_NANOS = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long RECONNECT_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final int MAX_FRAME = 16 << 20;
    private static final int MAX_PEER_BUFFER = 64 << 20;

    protected final int id;
    private final InetSocketAddress[] peers;
    protected final ExchangeNode node;
    private final Storage storage;

    protected final Selector selector;
    private final ServerSocketChannel peerListener;
    private final PeerLink[] outbound;
    private final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<>();

    /**
     * Raft bytes produced in one turn, waiting for that turn's fsync. When
     * the storage reports the token durable, each link may send up to its
     * mark. With a synchronous storage the token is durable at once.
     */
    private record Held(long token, FrameWriter[] writers, long[] marks) { }
    private final ArrayDeque<Held> held = new ArrayDeque<>();

    private volatile boolean running = true;
    private Thread thread;

    // Snapshots for other threads (tests, monitoring); written once per turn.
    private volatile Role roleSnapshot = Role.FOLLOWER;
    private volatile long termSnapshot;
    private volatile long commitSnapshot;
    private volatile long appliedSnapshot;
    private volatile int leaderSnapshot = -1;

    /** Outgoing connection to one peer. */
    private final class PeerLink {
        final int peer;
        SocketChannel channel;
        FrameWriter out;
        boolean connected;
        long retryAt;
        long releasable;                        // may send up to this many produced bytes

        PeerLink(int peer) {
            this.peer = peer;
        }
    }

    /** Incoming connection from a peer; its node id arrives in HELLO. */
    private static final class InboundPeer {
        final SocketChannel channel;
        final FrameReader in = new FrameReader(MAX_FRAME);
        int node = -1;

        InboundPeer(SocketChannel channel) {
            this.channel = channel;
        }
    }

    /**
     * @param peers peer-to-peer address of every node, indexed by node id,
     *              including this one (it listens on {@code peers[id]})
     */
    public NodeServer(int id, InetSocketAddress[] peers, Storage storage, RaftNode.Config config,
                      BookFactory books, ExecutionListener reports, ExchangeNode.AckListener acks)
            throws IOException {
        this.id = id;
        this.peers = peers;
        this.storage = storage;
        this.selector = Selector.open();
        this.peerListener = ServerSocketChannel.open();
        peerListener.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        peerListener.bind(peers[id]);
        peerListener.configureBlocking(false);
        peerListener.register(selector, SelectionKey.OP_ACCEPT, "peer-listener");
        this.outbound = new PeerLink[peers.length];
        for (int p = 0; p < peers.length; p++) {
            if (p != id) {
                outbound[p] = new PeerLink(p);
            }
        }
        this.node = new ExchangeNode(id, peers.length, storage, this::sendToPeer,
                new Random(System.nanoTime() ^ id), config, books, reports, acks);
        storage.onDurable(selector::wakeup);    // a finished fsync wakes the loop
    }

    public void start() {
        thread = new Thread(this::loop, "node-" + id);
        thread.start();
    }

    // --- the loop ---

    private void loop() {
        long nextTick = System.nanoTime() + TICK_NANOS;
        try {
            while (running) {
                long wait = TimeUnit.NANOSECONDS.toMillis(nextTick - System.nanoTime());
                if (wait > 0) {
                    selector.select(wait);
                } else {
                    selector.selectNow();
                }
                for (SelectionKey key : selector.selectedKeys()) {
                    handleKey(key);
                }
                selector.selectedKeys().clear();

                Runnable task;
                while ((task = tasks.poll()) != null) {
                    task.run();
                }

                long now = System.nanoTime();
                if (now >= nextTick) {
                    node.tick();
                    // Catch up if a turn ran long, but never tick in a burst.
                    nextTick = Math.max(nextTick + TICK_NANOS, now);
                    reconnectPeers(now);
                }
                afterInput();                   // subclass: gateway work for this turn
                node.flush();                   // smart batching: one Raft batch per turn
                holdUntilDurable(storage.requestSync());   // one fsync per turn
                flushWrites();
                publishSnapshots();
            }
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        } finally {
            closeChannels();
        }
    }

    private void handleKey(SelectionKey key) throws IOException {
        if (!key.isValid()) {
            return;
        }
        Object a = key.attachment();
        try {
            if (key.isAcceptable()) {
                accept((ServerSocketChannel) key.channel(), a);
            } else if (a instanceof PeerLink link) {
                if (key.isConnectable()) {
                    finishConnect(link, key);
                }
                if (key.isValid() && key.isReadable()) {
                    // Peers never write on our outgoing link; readable means closed.
                    if (link.channel.read(ByteBuffer.allocate(64)) < 0) {
                        dropLink(link);
                    }
                }
                // Writable: flushWrites() sends it at the end of this turn,
                // after the fsync. Peer bytes must never leave before it.
            } else if (a instanceof InboundPeer in) {
                if (key.isReadable()) {
                    readPeer(in, key);
                }
            } else {
                handleOtherKey(key);
            }
        } catch (IOException e) {
            onKeyFailure(key);
        }
    }

    private void accept(ServerSocketChannel server, Object which) throws IOException {
        SocketChannel ch = server.accept();
        if (ch == null) {
            return;
        }
        ch.configureBlocking(false);
        ch.setOption(StandardSocketOptions.TCP_NODELAY, true);  // no Nagle delay on small frames
        if ("peer-listener".equals(which)) {
            ch.register(selector, SelectionKey.OP_READ, new InboundPeer(ch));
        } else {
            acceptClient(ch);
        }
    }

    private void readPeer(InboundPeer in, SelectionKey key) throws IOException {
        if (in.in.readFrom(in.channel) < 0) {
            key.cancel();
            in.channel.close();
            return;
        }
        in.in.drain((buf, at, len) -> {
            if (buf.get(at) == PeerProtocol.HELLO) {
                in.node = PeerProtocol.helloNodeId(buf, at);
            } else {
                Message m = PeerProtocol.decode(buf, at, len);
                if (m.to() == id && m.from() == in.node) {
                    node.handle(m);
                }
            }
        });
    }

    // --- outgoing peer links ---

    private void sendToPeer(Message m) {
        PeerLink link = outbound[m.to()];
        if (link == null || link.out == null) {
            return;                             // not connected: Raft will retry
        }
        PeerProtocol.encode(link.out, m);
        if (link.out.pending() > MAX_PEER_BUFFER) {
            dropLink(link);                     // peer stopped reading; start over
        }
    }

    private void reconnectPeers(long now) throws IOException {
        for (PeerLink link : outbound) {
            if (link != null && link.channel == null && now >= link.retryAt) {
                SocketChannel ch = SocketChannel.open();
                ch.configureBlocking(false);
                ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
                link.channel = ch;
                link.out = new FrameWriter(MAX_PEER_BUFFER);
                PeerProtocol.hello(link.out, id);        // first frame on every link
                link.releasable = link.out.produced();   // HELLO can go at once
                try {
                    if (ch.connect(peers[link.peer])) {
                        link.connected = true;
                        ch.register(selector, SelectionKey.OP_READ, link);
                    } else {
                        ch.register(selector, SelectionKey.OP_CONNECT, link);
                    }
                } catch (IOException e) {
                    dropLink(link);
                }
            }
        }
    }

    private void finishConnect(PeerLink link, SelectionKey key) throws IOException {
        if (link.channel.finishConnect()) {
            link.connected = true;
            key.interestOps(SelectionKey.OP_READ);
        }
    }

    private void flushLink(PeerLink link) throws IOException {
        if (!link.connected || link.out.pending() == 0) {
            return;
        }
        boolean done = link.out.writeTo(link.channel, link.releasable);
        SelectionKey key = link.channel.keyFor(selector);
        key.interestOps(done ? SelectionKey.OP_READ : SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    }

    private void dropLink(PeerLink link) {
        try {
            if (link.channel != null) {
                link.channel.close();
            }
        } catch (IOException ignored) {
            // closing a broken socket
        }
        link.channel = null;
        link.out = null;
        link.connected = false;
        link.retryAt = System.nanoTime() + RECONNECT_NANOS;
    }

    /**
     * Marks everything the Raft node wrote to peers this turn as waiting for
     * {@code token}. Nobody may hear "I have entry N" before N is on disk.
     */
    private void holdUntilDurable(long token) {
        FrameWriter[] writers = new FrameWriter[outbound.length];
        long[] marks = new long[outbound.length];
        for (int p = 0; p < outbound.length; p++) {
            PeerLink link = outbound[p];
            if (link != null && link.out != null) {
                writers[p] = link.out;
                marks[p] = link.out.produced();
            }
        }
        held.add(new Held(token, writers, marks));
    }

    private void releaseDurable() {
        while (!held.isEmpty() && storage.isDurable(held.peek().token())) {
            Held h = held.poll();
            for (int p = 0; p < outbound.length; p++) {
                PeerLink link = outbound[p];
                if (link != null && link.out != null && link.out == h.writers()[p]) {
                    link.releasable = Math.max(link.releasable, h.marks()[p]);
                }
            }
        }
    }

    private void flushWrites() throws IOException {
        releaseDurable();
        for (PeerLink link : outbound) {
            if (link != null && link.channel != null) {
                try {
                    flushLink(link);
                } catch (IOException e) {
                    dropLink(link);
                }
            }
        }
        flushClients();
    }

    private void onKeyFailure(SelectionKey key) {
        Object a = key.attachment();
        if (a instanceof PeerLink link) {
            dropLink(link);
        } else {
            key.cancel();
            try {
                key.channel().close();
            } catch (IOException ignored) {
                // closing a broken socket
            }
            onClientClosed(a);
        }
    }

    // --- hooks for the gateway (step 3) ---

    protected void acceptClient(SocketChannel ch) throws IOException {
        ch.close();                             // plain node server takes no clients
    }

    protected void handleOtherKey(SelectionKey key) throws IOException { }

    protected void afterInput() { }

    protected void flushClients() throws IOException { }

    protected void onClientClosed(Object attachment) { }

    // --- other threads ---

    /** Runs {@code task} on the loop thread; the only safe way to touch the node. */
    public <T> CompletableFuture<T> call(Callable<T> task) {
        CompletableFuture<T> f = new CompletableFuture<>();
        tasks.add(() -> {
            try {
                f.complete(task.call());
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
        });
        selector.wakeup();
        return f;
    }

    private void publishSnapshots() {
        roleSnapshot = node.role();
        termSnapshot = node.term();
        commitSnapshot = node.commitIndex();
        appliedSnapshot = node.lastApplied();
        leaderSnapshot = node.leaderId();
    }

    public Role role() { return roleSnapshot; }
    public long term() { return termSnapshot; }
    public long commitIndex() { return commitSnapshot; }
    public long lastApplied() { return appliedSnapshot; }
    public int leaderId() { return leaderSnapshot; }
    public int nodeId() { return id; }

    /** Stops the loop, as if the process died: nothing is flushed on the way out. */
    @Override
    public void close() throws IOException {
        running = false;
        selector.wakeup();
        if (thread != null) {
            try {
                thread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (storage instanceof AutoCloseable c) {
            try {
                c.close();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    private void closeChannels() {
        List<java.nio.channels.Channel> open = new ArrayList<>();
        for (SelectionKey k : selector.keys()) {
            open.add(k.channel());
        }
        for (java.nio.channels.Channel c : open) {
            try {
                c.close();
            } catch (IOException ignored) {
                // shutting down
            }
        }
        try {
            selector.close();
        } catch (IOException ignored) {
            // shutting down
        }
    }
}
