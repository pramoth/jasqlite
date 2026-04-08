package com.jasqlite.store.page;

import com.jasqlite.util.BinaryUtils;

import java.io.*;
import java.util.Arrays;

/**
 * Write-Ahead Log (WAL) file implementation.
 *
 * WAL file format:
 * - WAL Header (32 bytes)
 * - Frame Header (24 bytes) + Page Data for each frame
 *
 * WAL Header:
 * Offset  Size  Description
 * 0       4     Magic number (0x377f0682 or 0x377f0683)
 * 4       4     File format version (3007000)
 * 8       4     Database page size
 * 12      4     Checkpoint sequence number
 * 16      4     Salt-1
 * 20      4     Salt-2
 * 24      4     Checksum-1
 * 28      4     Checksum-2
 *
 * Frame Header:
 * 0       4     Page number
 * 4       4     For commit records, size of database in pages after commit; 0 otherwise
 * 8       4     Salt-1 (must match WAL header)
 * 12      4     Salt-2 (must match WAL header)
 * 16      4     Checksum-1
 * 20      4     Checksum-2
 */
public class WALFile implements AutoCloseable {

    private static final int WAL_MAGIC_LE = 0x377f0682;
    private static final int WAL_MAGIC_BE = 0x377f0683;
    private static final int WAL_VERSION = 3007000;
    private static final int WAL_HEADER_SIZE = 32;
    private static final int FRAME_HEADER_SIZE = 24;

    private final RandomAccessFile file;
    private final int pageSize;
    private int checkpointSeqNo;
    private int salt1, salt2;
    private final String walPath;

    public WALFile(String dbPath, int pageSize) throws IOException {
        this.walPath = dbPath + "-wal";
        this.pageSize = pageSize;

        File f = new File(walPath);
        boolean isNew = !f.exists() || f.length() == 0;

        this.file = new RandomAccessFile(walPath, "rw");

        if (isNew) {
            this.checkpointSeqNo = 0;
            this.salt1 = (int) (System.currentTimeMillis() / 1000);
            this.salt2 = (int) (System.nanoTime() & 0xFFFFFFFFL);
            writeWALHeader();
        } else {
            readWALHeader();
        }
    }

    private void writeWALHeader() throws IOException {
        byte[] header = new byte[WAL_HEADER_SIZE];
        BinaryUtils.writeUInt32(header, 0, WAL_MAGIC_BE);
        BinaryUtils.writeUInt32(header, 4, WAL_VERSION);
        BinaryUtils.writeUInt32(header, 8, pageSize);
        BinaryUtils.writeUInt32(header, 12, checkpointSeqNo);
        BinaryUtils.writeUInt32(header, 16, salt1);
        BinaryUtils.writeUInt32(header, 20, salt2);
        // Checksums computed over first 24 bytes
        long cksum = BinaryUtils.walChecksum(header, 0, 24, 0, 0);
        BinaryUtils.writeUInt32(header, 24, cksum & 0xFFFFFFFFL);
        BinaryUtils.writeUInt32(header, 28, (cksum >> 32) & 0xFFFFFFFFL);
        file.seek(0);
        file.write(header);
    }

    private void readWALHeader() throws IOException {
        byte[] header = new byte[WAL_HEADER_SIZE];
        file.seek(0);
        file.readFully(header);

        long magic = BinaryUtils.readUInt32(header, 0);
        if (magic != WAL_MAGIC_BE && magic != WAL_MAGIC_LE) {
            throw new IOException("Invalid WAL magic number");
        }
        int version = (int) BinaryUtils.readUInt32(header, 4);
        int ps = (int) BinaryUtils.readUInt32(header, 8);
        if (ps != pageSize) {
            throw new IOException("WAL page size mismatch");
        }
        this.checkpointSeqNo = (int) BinaryUtils.readUInt32(header, 12);
        this.salt1 = (int) BinaryUtils.readUInt32(header, 16);
        this.salt2 = (int) BinaryUtils.readUInt32(header, 20);
    }

    /**
     * Write a page to the WAL.
     */
    public void writePage(int pageNumber, byte[] pageData) throws IOException {
        synchronized (file) {
            long offset = file.length();
            file.seek(offset);

            // Write frame header
            byte[] frameHeader = new byte[FRAME_HEADER_SIZE];
            BinaryUtils.writeUInt32(frameHeader, 0, pageNumber);
            BinaryUtils.writeUInt32(frameHeader, 4, 0); // not a commit frame yet
            BinaryUtils.writeUInt32(frameHeader, 8, salt1);
            BinaryUtils.writeUInt32(frameHeader, 12, salt2);

            // Compute checksum over frame header + page data
            long cksum = BinaryUtils.walChecksum(frameHeader, 0, 8, 0, 0);
            cksum = BinaryUtils.walChecksum(pageData, 0, pageSize, cksum & 0xFFFFFFFFL, (cksum >> 32) & 0xFFFFFFFFL);
            BinaryUtils.writeUInt32(frameHeader, 16, cksum & 0xFFFFFFFFL);
            BinaryUtils.writeUInt32(frameHeader, 20, (cksum >> 32) & 0xFFFFFFFFL);

            file.write(frameHeader);
            file.write(pageData);
        }
    }

    /**
     * Read the most recent version of a page from the WAL.
     */
    public byte[] readPage(int pageNumber) throws IOException {
        synchronized (file) {
            long fileSize = file.length();
            if (fileSize <= WAL_HEADER_SIZE) return null;

            int frameSize = FRAME_HEADER_SIZE + pageSize;
            long numFrames = (fileSize - WAL_HEADER_SIZE) / frameSize;

            // Search from end to find the latest version
            for (long i = numFrames - 1; i >= 0; i--) {
                long frameOffset = WAL_HEADER_SIZE + i * frameSize;
                file.seek(frameOffset);

                byte[] frameHeader = new byte[FRAME_HEADER_SIZE];
                file.readFully(frameHeader);

                int pgNo = (int) BinaryUtils.readUInt32(frameHeader, 0);
                int s1 = (int) BinaryUtils.readUInt32(frameHeader, 8);
                int s2 = (int) BinaryUtils.readUInt32(frameHeader, 12);

                if (pgNo == pageNumber && s1 == salt1 && s2 == salt2) {
                    byte[] data = new byte[pageSize];
                    file.readFully(data);
                    return data;
                }
            }
            return null;
        }
    }

    /**
     * Commit the WAL - write a commit frame.
     */
    public void commit() throws IOException {
        synchronized (file) {
            long offset = file.length();
            file.seek(offset);

            // Write commit frame header (empty page, signals commit)
            byte[] frameHeader = new byte[FRAME_HEADER_SIZE];
            BinaryUtils.writeUInt32(frameHeader, 0, 0); // page 0 = commit marker
            BinaryUtils.writeUInt32(frameHeader, 4, 0); // db size (0 for now)
            BinaryUtils.writeUInt32(frameHeader, 8, salt1);
            BinaryUtils.writeUInt32(frameHeader, 12, salt2);

            long cksum = BinaryUtils.walChecksum(frameHeader, 0, 8, 0, 0);
            BinaryUtils.writeUInt32(frameHeader, 16, cksum & 0xFFFFFFFFL);
            BinaryUtils.writeUInt32(frameHeader, 20, (cksum >> 32) & 0xFFFFFFFFL);

            file.write(frameHeader);
            file.getFD().sync();
        }
    }

    /**
     * Run a checkpoint - copy WAL pages back to the main database file.
     */
    public void checkpoint(RandomAccessFile dbFile, int dbPageCount) throws IOException {
        synchronized (file) {
            long fileSize = file.length();
            if (fileSize <= WAL_HEADER_SIZE) return;

            int frameSize = FRAME_HEADER_SIZE + pageSize;
            long numFrames = (fileSize - WAL_HEADER_SIZE) / frameSize;

            for (long i = 0; i < numFrames; i++) {
                long frameOffset = WAL_HEADER_SIZE + i * frameSize;
                file.seek(frameOffset);

                byte[] frameHeader = new byte[FRAME_HEADER_SIZE];
                file.readFully(frameHeader);

                int pgNo = (int) BinaryUtils.readUInt32(frameHeader, 0);
                if (pgNo == 0) continue; // commit frame

                int s1 = (int) BinaryUtils.readUInt32(frameHeader, 8);
                int s2 = (int) BinaryUtils.readUInt32(frameHeader, 12);
                if (s1 != salt1 || s2 != salt2) break;

                byte[] pageData = new byte[pageSize];
                file.readFully(pageData);

                if (pgNo <= dbPageCount) {
                    dbFile.seek((long) (pgNo - 1) * pageSize);
                    dbFile.write(pageData);
                }
            }

            dbFile.getFD().sync();

            // Truncate WAL
            file.setLength(0);
            checkpointSeqNo++;
            salt1 = (int) (System.currentTimeMillis() / 1000);
            salt2 = (int) (System.nanoTime() & 0xFFFFFFFFL);
            writeWALHeader();
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (file) {
            file.close();
        }
    }

    public void delete() throws IOException {
        file.close();
        new File(walPath).delete();
    }
}
