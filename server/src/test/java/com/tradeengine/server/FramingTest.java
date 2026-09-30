package com.tradeengine.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ProtocolException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** TCP splits and merges bytes however it likes; frames must come out whole. */
class FramingTest {

    /** Serves a byte array in chunks of the given sizes, like successive TCP reads. */
    static final class ChunkedChannel implements ReadableByteChannel {
        private final byte[] data;
        private final int[] chunks;
        private int pos, next;

        ChunkedChannel(byte[] data, int... chunks) {
            this.data = data;
            this.chunks = chunks;
        }

        @Override
        public int read(ByteBuffer dst) {
            if (pos == data.length) {
                return -1;
            }
            int n = Math.min(Math.min(chunks[next++ % chunks.length], data.length - pos), dst.remaining());
            dst.put(data, pos, n);
            pos += n;
            return n;
        }

        @Override public boolean isOpen() { return true; }
        @Override public void close() { }
    }

    /** Accepts at most {@code perCall} bytes per write, like a full socket buffer. */
    static final class SlowChannel implements WritableByteChannel {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private final int perCall;

        SlowChannel(int perCall) {
            this.perCall = perCall;
        }

        @Override
        public int write(ByteBuffer src) {
            int n = Math.min(perCall, src.remaining());
            byte[] b = new byte[n];
            src.get(b);
            out.write(b, 0, n);
            return n;
        }

        @Override public boolean isOpen() { return true; }
        @Override public void close() { }
    }

    private static byte[] framesOf(int count, int fieldBytes) throws IOException {
        FrameWriter w = new FrameWriter(1 << 30);
        for (int i = 0; i < count; i++) {
            ByteBuffer b = w.begin((byte) 'T', fieldBytes).putInt(i);
            for (int k = 4; k < fieldBytes; k++) {
                b.put((byte) (i + k));
            }
            w.end();
        }
        SlowChannel sink = new SlowChannel(Integer.MAX_VALUE);
        w.writeTo(sink);
        return sink.out.toByteArray();
    }

    private static List<Integer> readAll(ReadableByteChannel ch, FrameReader r) throws IOException {
        List<Integer> seen = new ArrayList<>();
        while (r.readFrom(ch) >= 0) {
            r.drain((buf, at, len) -> {
                assertEquals('T', buf.get(at));
                seen.add(buf.getInt(at + 1));
            });
        }
        r.drain((buf, at, len) -> seen.add(buf.getInt(at + 1)));
        return seen;
    }

    @Test
    void framesSplitOneByteAtATime() throws IOException {
        byte[] bytes = framesOf(20, 30);
        List<Integer> seen = readAll(new ChunkedChannel(bytes, 1), new FrameReader(1024));
        assertEquals(20, seen.size());
        for (int i = 0; i < 20; i++) {
            assertEquals(i, seen.get(i));
        }
    }

    @Test
    void manyFramesInOneRead() throws IOException {
        byte[] bytes = framesOf(500, 12);
        List<Integer> seen = readAll(new ChunkedChannel(bytes, bytes.length), new FrameReader(1024));
        assertEquals(500, seen.size());
    }

    @Test
    void randomSplitsAlwaysGiveTheSameFrames() throws IOException {
        byte[] bytes = framesOf(300, 40);
        Random rnd = new Random(7);
        for (int trial = 0; trial < 50; trial++) {
            int[] chunks = new int[17];
            for (int i = 0; i < chunks.length; i++) {
                chunks[i] = 1 + rnd.nextInt(120);
            }
            List<Integer> seen = readAll(new ChunkedChannel(bytes, chunks), new FrameReader(1024));
            assertEquals(300, seen.size(), "trial " + trial);
            assertEquals(299, seen.get(299));
        }
    }

    @Test
    void frameBiggerThanTheStartingBufferStillArrives() throws IOException {
        byte[] bytes = framesOf(2, 300_000);          // starting buffer is 64 KB
        List<Integer> seen = readAll(new ChunkedChannel(bytes, 4096), new FrameReader(1 << 20));
        assertEquals(List.of(0, 1), seen);
    }

    @Test
    void absurdLengthIsAProtocolError() {
        byte[] bytes = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F, 'T'};
        FrameReader r = new FrameReader(1024);
        assertThrows(ProtocolException.class, () -> readAll(new ChunkedChannel(bytes, 5), r));
    }

    @Test
    void slowSocketGetsEveryByteEventually() throws IOException {
        FrameWriter w = new FrameWriter(1 << 20);
        for (int i = 0; i < 100; i++) {
            w.begin((byte) 'T', 8).putInt(i).putInt(-i);
            w.end();
        }
        SlowChannel sink = new SlowChannel(7);
        int calls = 0;
        while (!w.writeTo(sink)) {
            calls++;
        }
        assertTrue(calls > 10, "should have needed many partial writes");
        List<Integer> seen = readAll(new ChunkedChannel(sink.out.toByteArray(), 13), new FrameReader(1024));
        assertEquals(100, seen.size());
        assertEquals(0, w.pending());
    }

    @Test
    void writerReportsASlowReader() {
        FrameWriter w = new FrameWriter(1_000);
        for (int i = 0; i < 100; i++) {
            w.begin((byte) 'T', 16).putLong(i).putLong(i);
            w.end();
        }
        assertTrue(w.overLimit());
    }
}
