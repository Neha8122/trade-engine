package com.tradeengine.feed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.tradeengine.orderbook.Side;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;

class CodecTest {

    private final ByteBuffer b = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);

    @Test
    void addRoundTrip() {
        assertEquals(26, Codec.encodeAdd(b, 5, 123_456_789L, Side.SELL, 100_050, 700));
        assertEquals(Codec.ADD, Codec.type(b, 5));
        assertEquals(26, Codec.size(b, 5));
        assertEquals(123_456_789L, Codec.orderId(b, 5));
        assertEquals(Side.SELL, Codec.addSide(b, 5));
        assertEquals(100_050, Codec.addPrice(b, 5));
        assertEquals(700, Codec.addQty(b, 5));
    }

    @Test
    void executeRoundTrip() {
        assertEquals(25, Codec.encodeExecute(b, 0, 42, 300, 100_051));
        assertEquals(Codec.EXECUTE, Codec.type(b, 0));
        assertEquals(42, Codec.orderId(b, 0));
        assertEquals(300, Codec.executeQty(b, 0));
        assertEquals(100_051, Codec.executePrice(b, 0));
    }

    @Test
    void deleteRoundTrip() {
        assertEquals(9, Codec.encodeDelete(b, 3, 99));
        assertEquals(Codec.DELETE, Codec.type(b, 3));
        assertEquals(99, Codec.orderId(b, 3));
    }

    @Test
    void fieldsSitAtTheDocumentedOffsetsLittleEndian() {
        Codec.encodeAdd(b, 0, 0x0102030405060708L, Side.BUY, 1, 2);
        assertEquals('A', b.get(0));
        assertEquals('B', b.get(1));
        assertEquals(0x08, b.get(2));   // lowest byte first
        assertEquals(0x01, b.get(9));
        assertEquals(1, b.getLong(10));
        assertEquals(2, b.getLong(18));
    }

    @Test
    void headerRoundTrip() {
        Codec.writeHeader(b, 1_000_001, 54);
        assertEquals(1_000_001, Codec.seqNo(b));
        assertEquals(54, Codec.count(b));
    }

    @Test
    void unknownTypeIsRejected() {
        b.put(0, (byte) 'Z');
        assertThrows(IllegalArgumentException.class, () -> Codec.size(b, 0));
    }
}
