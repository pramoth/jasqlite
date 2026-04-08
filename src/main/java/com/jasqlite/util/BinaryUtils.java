package com.jasqlite.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Binary utilities for reading/writing SQLite file format structures.
 * All multi-byte integers in SQLite are big-endian.
 */
public final class BinaryUtils {

    private BinaryUtils() {}

    public static int readUInt16(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
    }

    public static void writeUInt16(byte[] data, int offset, int value) {
        data[offset] = (byte) ((value >> 8) & 0xFF);
        data[offset + 1] = (byte) (value & 0xFF);
    }

    public static long readUInt32(byte[] data, int offset) {
        return ((long)(data[offset] & 0xFF) << 24) |
               ((long)(data[offset + 1] & 0xFF) << 16) |
               ((long)(data[offset + 2] & 0xFF) << 8) |
               ((long)(data[offset + 3] & 0xFF));
    }

    public static void writeUInt32(byte[] data, int offset, long value) {
        data[offset]     = (byte) ((value >> 24) & 0xFF);
        data[offset + 1] = (byte) ((value >> 16) & 0xFF);
        data[offset + 2] = (byte) ((value >> 8) & 0xFF);
        data[offset + 3] = (byte) (value & 0xFF);
    }

    public static long readUInt40(byte[] data, int offset) {
        return ((long)(data[offset] & 0xFF) << 32) |
               ((long)(data[offset + 1] & 0xFF) << 24) |
               ((long)(data[offset + 2] & 0xFF) << 16) |
               ((long)(data[offset + 3] & 0xFF) << 8) |
               ((long)(data[offset + 4] & 0xFF));
    }

    public static void writeUInt40(byte[] data, int offset, long value) {
        data[offset]     = (byte) ((value >> 32) & 0xFF);
        data[offset + 1] = (byte) ((value >> 24) & 0xFF);
        data[offset + 2] = (byte) ((value >> 16) & 0xFF);
        data[offset + 3] = (byte) ((value >> 8) & 0xFF);
        data[offset + 4] = (byte) (value & 0xFF);
    }

    public static long readUInt48(byte[] data, int offset) {
        return ((long)(data[offset] & 0xFF) << 40) |
               ((long)(data[offset + 1] & 0xFF) << 32) |
               ((long)(data[offset + 2] & 0xFF) << 24) |
               ((long)(data[offset + 3] & 0xFF) << 16) |
               ((long)(data[offset + 4] & 0xFF) << 8) |
               ((long)(data[offset + 5] & 0xFF));
    }

    public static void writeUInt48(byte[] data, int offset, long value) {
        data[offset]     = (byte) ((value >> 40) & 0xFF);
        data[offset + 1] = (byte) ((value >> 32) & 0xFF);
        data[offset + 2] = (byte) ((value >> 24) & 0xFF);
        data[offset + 3] = (byte) ((value >> 16) & 0xFF);
        data[offset + 4] = (byte) ((value >> 8) & 0xFF);
        data[offset + 5] = (byte) (value & 0xFF);
    }

    /**
     * Read a SQLite varint (variable-length integer).
     * Returns a long[2] where [0] is the value and [1] is the number of bytes consumed.
     */
    public static long[] readVarInt(byte[] data, int offset) {
        long result = 0;
        for (int i = 0; i < 9; i++) {
            byte b = data[offset + i];
            if (i < 8) {
                result = (result << 7) | (b & 0x7F);
                if ((b & 0x80) == 0) {
                    return new long[]{result, i + 1};
                }
            } else {
                // 9th byte uses all 8 bits
                result = (result << 8) | (b & 0xFF);
                return new long[]{result, 9};
            }
        }
        return new long[]{result, 9};
    }

    /**
     * Write a SQLite varint. Returns the number of bytes written.
     */
    public static int writeVarInt(byte[] data, int offset, long value) {
        if (value < 0) {
            // Negative values need 9 bytes
            data[offset] = (byte) 0x80;
            for (int i = 0; i < 8; i++) {
                data[offset + 1 + i] = (byte) ((value >> (56 - i * 8)) & 0xFF);
            }
            return 9;
        }
        byte[] buf = new byte[10];
        int len = 0;
        buf[len++] = (byte) (value & 0x7F);
        value >>>= 7;
        while (value > 0) {
            buf[len++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        // Reverse into output
        for (int i = 0; i < len; i++) {
            data[offset + i] = buf[len - 1 - i];
        }
        return len;
    }

    /**
     * Compute the size of a varint for the given value.
     */
    public static int varIntSize(long value) {
        if (value < 0) return 9;
        if (value < 128) return 1;
        if (value < 16384) return 2;
        if (value < 2097152) return 3;
        if (value < 268435456) return 4;
        if (value < 34359738368L) return 5;
        if (value < 4398046511104L) return 6;
        if (value < 562949953421312L) return 7;
        if (value < 72057594037927936L) return 8;
        return 9;
    }

    public static byte[] longToBytes(long value) {
        byte[] result = new byte[8];
        for (int i = 0; i < 8; i++) {
            result[i] = (byte) ((value >> (56 - i * 8)) & 0xFF);
        }
        return result;
    }

    public static long bytesToLong(byte[] data, int offset) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result = (result << 8) | (data[offset + i] & 0xFF);
        }
        return result;
    }

    public static byte[] intToBytes(int value) {
        byte[] result = new byte[4];
        result[0] = (byte) ((value >> 24) & 0xFF);
        result[1] = (byte) ((value >> 16) & 0xFF);
        result[2] = (byte) ((value >> 8) & 0xFF);
        result[3] = (byte) (value & 0xFF);
        return result;
    }

    /**
     * Compute the checksum used in WAL frames.
     */
    public static long walChecksum(byte[] data, int offset, int length, long s0, long s1) {
        for (int i = 0; i < length; i += 8) {
            long v = readUInt32(data, offset + i);
            s0 += v + s1;
            long v2 = readUInt32(data, offset + i + 4);
            s1 += v2 + s0;
        }
        return (s0 & 0xFFFFFFFFL) | ((s1 & 0xFFFFFFFFL) << 32);
    }
}
