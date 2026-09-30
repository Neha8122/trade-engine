package com.tradeengine.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Outgoing bytes for one connection. Frames are encoded straight into one
 * buffer and written to the socket in as few system calls as possible:
 * everything produced in one event-loop turn goes out together.
 *
 * <p>Usage: {@code ByteBuffer b = begin(type, maxFields); b.putLong(...); end();}
 */
public final class FrameWriter {

    private final int maxBuffered;
    private ByteBuffer buf;
    private int frameStart = -1;

    /** @param maxBuffered past this many unsent bytes the peer is too slow; see {@link #overLimit()} */
    public FrameWriter(int maxBuffered) {
        this.maxBuffered = maxBuffered;
        this.buf = ByteBuffer.allocate(64 * 1024).order(ByteOrder.LITTLE_ENDIAN);
    }

    /** Starts a frame; returns the buffer to put the fields into. */
    public ByteBuffer begin(byte type, int maxFields) {
        ensure(4 + 1 + maxFields);
        frameStart = buf.position();
        buf.putInt(0).put(type);
        return buf;
    }

    /** Finishes the frame started by {@link #begin}: fills in its length. */
    public void end() {
        buf.putInt(frameStart, buf.position() - frameStart - 4);
        frameStart = -1;
    }

    /** Writes as much as the socket takes. True if nothing is left waiting. */
    public boolean writeTo(WritableByteChannel channel) throws IOException {
        buf.flip();
        try {
            channel.write(buf);
        } finally {
            buf.compact();
        }
        return buf.position() == 0;
    }

    public int pending() {
        return buf.position();
    }

    /** A reader this far behind is dropped rather than buffered forever. */
    public boolean overLimit() {
        return buf.position() > maxBuffered;
    }

    private void ensure(int bytes) {
        if (buf.remaining() < bytes) {
            int size = buf.capacity();
            while (size - buf.position() < bytes) {
                size *= 2;
            }
            ByteBuffer bigger = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
            buf.flip();
            bigger.put(buf);
            buf = bigger;
        }
    }
}
