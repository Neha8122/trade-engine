package com.tradeengine.server;

import static com.tradeengine.orderbook.OrderType.IOC;
import static com.tradeengine.orderbook.OrderType.LIMIT;
import static com.tradeengine.orderbook.Side.BUY;
import static com.tradeengine.orderbook.Side.SELL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradeengine.exchange.BookFactory;
import com.tradeengine.orderbook.OrderBook;
import com.tradeengine.orderbook.RejectReason;
import com.tradeengine.raft.FileStorage;
import com.tradeengine.raft.RaftNode;
import com.tradeengine.raft.Role;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Real clients over TCP against three real gateway servers. */
class GatewayTest {

    private static final BookFactory BOOKS = r -> new OrderBook(1, 100_000, 200, 10_000, r);
    private static final GatewayServer.Limits LIMITS = new GatewayServer.Limits(1_000, 20, 3, 10_000, 1_000);

    @TempDir
    Path dir;

    private final GatewayServer[] servers = new GatewayServer[3];
    private final InetSocketAddress[] clientAddr = new InetSocketAddress[3];
    private int leader;

    @BeforeEach
    void start() throws IOException {
        InetSocketAddress[] peers = new InetSocketAddress[3];
        for (int i = 0; i < 3; i++) {
            peers[i] = freePort();
            clientAddr[i] = freePort();
        }
        for (int i = 0; i < 3; i++) {
            servers[i] = new GatewayServer(i, peers, clientAddr[i], FileStorage.groupCommit(dir.resolve("n" + i)),
                    RaftNode.Config.DEFAULT, BOOKS, LIMITS);
            servers[i].start();
        }
        leader = awaitLeader();
    }

    @AfterEach
    void stop() throws IOException {
        for (GatewayServer s : servers) {
            s.close();
        }
    }

    @Test
    void twoClientsTradeAndBothGetFills() throws IOException {
        try (TestClient seller = new TestClient(clientAddr[leader]).logon(1);
             TestClient buyer = new TestClient(clientAddr[leader]).logon(2)) {

            seller.order(11, SELL, LIMIT, 100_100, 10);
            long sellId = TestClient.orderId(seller.await("ACK clOrd=11"));

            buyer.order(21, BUY, IOC, 100_100, 4);
            long buyId = TestClient.orderId(buyer.await("ACK clOrd=21"));

            buyer.await("FILL order=" + buyId + " 4@100100");
            seller.await("FILL order=" + sellId + " 4@100100");

            // The seller's remaining 6 are still resting; cancel them.
            seller.cancel(12, sellId);
            seller.await("CANCELLED order=" + sellId + " leaves=6");
        }
    }

    @Test
    void followerRedirectsToTheLeader() throws IOException {
        int follower = (leader + 1) % 3;
        try (TestClient c = new TestClient(clientAddr[follower]).logon(1)) {
            c.order(1, BUY, LIMIT, 100_090, 1);
            assertEquals("NOT_LEADER " + leader, c.await("NOT_LEADER"));
        }
        try (TestClient c = new TestClient(clientAddr[leader]).logon(1)) {
            c.order(1, BUY, LIMIT, 100_090, 1);
            c.await("ACK clOrd=1");
        }
    }

    @Test
    void ordersBeforeLogonAreRejected() throws IOException {
        try (TestClient c = new TestClient(clientAddr[leader])) {
            c.order(5, BUY, LIMIT, 100_090, 1);
            c.await("REJECT clOrd=5 reason=" + ClientProtocol.NOT_LOGGED_ON);
        }
    }

    @Test
    void riskChecksRejectBeforeTheLog() throws IOException {
        try (TestClient a = new TestClient(clientAddr[leader]).logon(1);
             TestClient b = new TestClient(clientAddr[leader]).logon(2)) {

            a.order(1, BUY, LIMIT, 100_100, 5_000);             // over max size 1,000
            a.await("REJECT clOrd=1 reason=" + ClientProtocol.RISK_MAX_QTY);

            a.order(2, SELL, LIMIT, 100_100, 1);                // make a trade at 1001.00
            a.await("ACK clOrd=2");     // separate connections: TCP doesn't order them
            b.order(1, BUY, IOC, 100_100, 1);
            b.await("FILL");
            b.order(2, BUY, LIMIT, 100_150, 1);                 // 50 ticks away, collar is 20
            b.await("REJECT clOrd=2 reason=" + ClientProtocol.RISK_PRICE_COLLAR);

            for (long id = 10; id < 13; id++) {                 // 3 resting: at the limit
                a.order(id, BUY, LIMIT, 100_090, 1);
                a.await("ACK clOrd=" + id);
            }
            a.order(13, BUY, LIMIT, 100_090, 1);
            a.await("REJECT clOrd=13 reason=" + ClientProtocol.RISK_OPEN_ORDERS);
        }
    }

    @Test
    void filledOrdersDontCountAsOpen() throws IOException {
        try (TestClient a = new TestClient(clientAddr[leader]).logon(1);
             TestClient b = new TestClient(clientAddr[leader]).logon(2)) {
            // Client 1 rests and gets filled 5 times: its open count must go back to 0 each time.
            for (long id = 1; id <= 5; id++) {
                a.order(id, SELL, LIMIT, 100_100, 1);
                a.await("ACK clOrd=" + id);
                b.order(id, BUY, IOC, 100_100, 1);
                b.await("ACK clOrd=" + id);
            }
            a.order(99, SELL, LIMIT, 100_100, 1);
            a.await("ACK clOrd=99");                            // not rejected: nothing open
        }
    }

    @Test
    void cannotCancelSomeoneElsesOrder() throws IOException {
        try (TestClient a = new TestClient(clientAddr[leader]).logon(1);
             TestClient b = new TestClient(clientAddr[leader]).logon(2)) {
            a.order(1, SELL, LIMIT, 100_110, 3);
            long mine = TestClient.orderId(a.await("ACK clOrd=1"));

            b.cancel(7, mine);
            b.await("REJECT clOrd=7 reason=" + RejectReason.UNKNOWN_ORDER.ordinal());

            a.cancel(2, mine);
            a.await("CANCELLED order=" + mine + " leaves=3");
            assertTrue(b.received.stream().noneMatch(l -> l.startsWith("CANCELLED")));
        }
    }

    // --- helpers ---

    private int awaitLeader() {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            int found = -1;
            int count = 0;
            for (int i = 0; i < 3; i++) {
                if (servers[i].role() == Role.LEADER) {
                    found = i;
                    count++;
                }
            }
            boolean followersAgree = found != -1;
            for (int i = 0; i < 3 && followersAgree; i++) {
                followersAgree = i == found || servers[i].leaderId() == found;
            }
            if (count == 1 && followersAgree && servers[found].commitIndex() > 0) {
                return found;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
        throw new AssertionError("no stable leader");
    }

    private static InetSocketAddress freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return new InetSocketAddress(InetAddress.getLoopbackAddress(), s.getLocalPort());
        }
    }
}
