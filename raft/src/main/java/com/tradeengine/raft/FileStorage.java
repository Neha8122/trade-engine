package com.tradeengine.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Raft's durable state in two files in one directory.
 *
 * <p><b>{@code meta}</b>: term and vote. Two fixed 32-byte slots written
 * alternately, each with a version number and a checksum. A crash halfway
 * through a write can only damage the slot being written; on startup the
 * valid slot with the higher version wins, so the previous term and vote
 * survive. No rename or directory sync needed.
 *
 * <p><b>{@code log}</b>: append-only records, one per entry:
 * {@code [length u32][crc32c u32][term u64][command bytes]}. On startup the
 * file is read front to back; the first record that is incomplete or fails
 * its checksum marks where a crash cut a write short, and the file is
 * truncated there. Those entries were never acknowledged (we fsync before
 * replying), so dropping them is safe.
 *
 * <p>Every change is fsynced before the method returns. {@link #append}
 * writes a whole batch with one fsync: group commit.
 *
 * <p>Note: on macOS, {@code FileChannel.force} is {@code fsync}, which
 * doesn't flush the drive's own cache ({@code F_FULLFSYNC} would). On Linux
 * it reaches the disk.
 */
public final class FileStorage implements Storage, AutoCloseable {

    private static final int SLOT = 32;
    private static final int SLOT_BODY = 20;        // version 8 + term 8 + vote 4
    private static final int RECORD_HEADER = 8;     // length 4 + crc 4

    private final FileChannel meta;
    private final FileChannel log;
    private final boolean groupCommit;
    private boolean dirty;

    private long metaVersion;
    private long currentTerm;
    private int votedFor = -1;

    private final List<LogEntry> entries = new ArrayList<>();   // index 1 is entries.get(0)
    private long[] offsets = new long[1024];                     // file offset of each entry
    private long logEnd;

    public FileStorage(Path dir) {
        this(dir, false);
    }

    /**
     * Group commit: log appends and truncations are written but not fsynced
     * until {@link #sync()}, so one fsync covers everything a server did in
     * one event-loop turn. The server calls {@code sync()} before sending
     * any message, so nobody is ever told about an entry that isn't on disk.
     * Term and vote changes are always fsynced at once.
     */
    public static FileStorage groupCommit(Path dir) {
        return new FileStorage(dir, true);
    }

    private FileStorage(Path dir, boolean groupCommit) {
        this.groupCommit = groupCommit;
        try {
            Files.createDirectories(dir);
            meta = FileChannel.open(dir.resolve("meta"),
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            log = FileChannel.open(dir.resolve("log"),
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            loadMeta();
            recoverLog();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // --- term and vote ---

    @Override public long currentTerm() { return currentTerm; }
    @Override public int votedFor() { return votedFor; }

    @Override
    public void saveTermAndVote(long term, int votedFor) {
        long version = metaVersion + 1;
        ByteBuffer b = ByteBuffer.allocate(SLOT);
        b.putLong(version).putLong(term).putInt(votedFor);
        b.putInt(crc(b.array(), 0, SLOT_BODY));
        b.clear();
        try {
            writeFully(meta, b, (version & 1) * SLOT);   // alternate slots
            meta.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        metaVersion = version;
        currentTerm = term;
        this.votedFor = votedFor;
    }

    private void loadMeta() throws IOException {
        ByteBuffer b = ByteBuffer.allocate(2 * SLOT);
        meta.read(b, 0);
        byte[] a = b.array();
        for (int slot = 0; slot < 2; slot++) {
            int at = slot * SLOT;
            ByteBuffer s = ByteBuffer.wrap(a, at, SLOT);
            long version = s.getLong();
            long term = s.getLong();
            int vote = s.getInt();
            int crc = s.getInt();
            if (version > metaVersion && crc == crc(a, at, SLOT_BODY)) {
                metaVersion = version;
                currentTerm = term;
                votedFor = vote;
            }
        }
    }

    // --- log ---

    @Override public long lastIndex() { return entries.size(); }

    @Override
    public long termAt(long index) {
        return index == 0 ? 0 : entries.get((int) index - 1).term();
    }

    @Override
    public LogEntry entry(long index) {
        return entries.get((int) index - 1);
    }

    @Override
    public void append(List<LogEntry> batch) {
        if (batch.isEmpty()) {
            return;
        }
        int bytes = 0;
        for (LogEntry e : batch) {
            bytes += RECORD_HEADER + 8 + e.command().length;
        }
        ByteBuffer b = ByteBuffer.allocate(bytes);
        long[] starts = new long[batch.size()];
        long pos = logEnd;
        for (int i = 0; i < batch.size(); i++) {
            LogEntry e = batch.get(i);
            int bodyLen = 8 + e.command().length;
            int bodyStart = b.position() + RECORD_HEADER;
            starts[i] = pos;
            b.putInt(bodyLen).putInt(0).putLong(e.term()).put(e.command());
            b.putInt(bodyStart - 4, crc(b.array(), bodyStart, bodyLen));
            pos += RECORD_HEADER + bodyLen;
        }
        b.flip();
        try {
            writeFully(log, b, logEnd);
            forceLog();                         // one fsync for the whole batch (or turn)
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        for (int i = 0; i < batch.size(); i++) {
            addOffset(starts[i]);
            entries.add(batch.get(i));
        }
        logEnd = pos;
    }

    @Override
    public void truncateFrom(long index) {
        long cut = offsets[(int) index - 1];
        try {
            log.truncate(cut);
            forceLog();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        entries.subList((int) index - 1, entries.size()).clear();
        logEnd = cut;
    }

    /** Reads every intact record; cuts the file at the first damaged one. */
    private void recoverLog() throws IOException {
        long size = log.size();
        long pos = 0;
        ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER);
        while (pos + RECORD_HEADER <= size) {
            header.clear();
            readFully(log, header, pos);
            int bodyLen = header.getInt(0);
            int crc = header.getInt(4);
            if (bodyLen < 8 || pos + RECORD_HEADER + bodyLen > size) {
                break;                          // torn write at the tail
            }
            ByteBuffer body = ByteBuffer.allocate(bodyLen);
            readFully(log, body, pos + RECORD_HEADER);
            if (crc != crc(body.array(), 0, bodyLen)) {
                break;                          // damaged record
            }
            long term = body.getLong(0);
            byte[] command = Arrays.copyOfRange(body.array(), 8, bodyLen);
            addOffset(pos);
            entries.add(new LogEntry(term, command));
            pos += RECORD_HEADER + bodyLen;
        }
        if (pos < size) {
            log.truncate(pos);
            log.force(false);
        }
        logEnd = pos;
    }

    private void forceLog() throws IOException {
        if (groupCommit) {
            dirty = true;
        } else {
            log.force(false);
        }
    }

    @Override
    public void sync() {
        if (dirty) {
            try {
                log.force(false);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            dirty = false;
        }
    }

    private void addOffset(long offset) {
        int i = entries.size();
        if (i == offsets.length) {
            offsets = Arrays.copyOf(offsets, i * 2);
        }
        offsets[i] = offset;
    }

    // --- helpers ---

    private static int crc(byte[] a, int off, int len) {
        CRC32C c = new CRC32C();
        c.update(a, off, len);
        return (int) c.getValue();
    }

    private static void writeFully(FileChannel ch, ByteBuffer b, long at) throws IOException {
        while (b.hasRemaining()) {
            at += ch.write(b, at);
        }
    }

    private static void readFully(FileChannel ch, ByteBuffer b, long at) throws IOException {
        while (b.hasRemaining()) {
            int n = ch.read(b, at + b.position());
            if (n < 0) {
                throw new IOException("unexpected end of file");
            }
        }
    }

    @Override
    public void close() throws IOException {
        meta.close();
        log.close();
    }
}
