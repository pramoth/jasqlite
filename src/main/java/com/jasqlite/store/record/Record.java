package com.jasqlite.store.record;

import com.jasqlite.util.BinaryUtils;
import com.jasqlite.util.SQLiteValue;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads and writes SQLite record format.
 *
 * A record consists of a header and a body.
 * The header begins with a varint that gives the total size of the header
 * (including the size varint itself). Following the size varint are one or
 * more additional varints, one per column. These additional varints are
 * called "serial type codes" and determine the size and type of the
 * corresponding column value in the body.
 *
 * Serial Type Codes:
 * 0       -> NULL, 0 bytes
 * 1       -> 8-bit twos-complement integer, 1 byte
 * 2       -> 16-bit big-endian twos-complement integer, 2 bytes
 * 3       -> 24-bit big-endian twos-complement integer, 3 bytes
 * 4       -> 32-bit big-endian twos-complement integer, 4 bytes
 * 5       -> 48-bit big-endian twos-complement integer, 6 bytes
 * 6       -> 64-bit big-endian twos-complement integer, 8 bytes
 * 7       -> IEEE 754 64-bit float, 8 bytes
 * 8       -> integer 0, 0 bytes
 * 9       -> integer 1, 0 bytes
 * 10, 11  -> Reserved
 * N >= 12, even -> BLOB of length (N-12)/2
 * N >= 13, odd  -> TEXT of length (N-13)/2
 */
public class Record {

    private final List<SQLiteValue> values;

    public Record() {
        this.values = new ArrayList<>();
    }

    public Record(List<SQLiteValue> values) {
        this.values = new ArrayList<>(values);
    }

    public void addValue(SQLiteValue value) {
        values.add(value);
    }

    public List<SQLiteValue> getValues() {
        return values;
    }

    public SQLiteValue getValue(int index) {
        if (index < 0 || index >= values.size()) return SQLiteValue.fromNull();
        return values.get(index);
    }

    public int getColumnCount() {
        return values.size();
    }

    /**
     * Serialize this record into the SQLite record format.
     * Returns the complete record as a byte array.
     */
    public byte[] serialize() {
        ByteArrayOutputStream headerBuf = new ByteArrayOutputStream();
        ByteArrayOutputStream bodyBuf = new ByteArrayOutputStream();

        // We'll compute the header size after encoding
        List<byte[]> serialTypeVarInts = new ArrayList<>();
        int headerContentSize = 0;

        for (SQLiteValue value : values) {
            int serialType = computeSerialType(value);
            byte[] stBytes = new byte[BinaryUtils.varIntSize(serialType)];
            BinaryUtils.writeVarInt(stBytes, 0, serialType);
            serialTypeVarInts.add(stBytes);
            headerContentSize += stBytes.length;
        }

        // Total header size = size varint + content
        int totalHeaderSize = headerContentSize + BinaryUtils.varIntSize(headerContentSize + BinaryUtils.varIntSize(headerContentSize));

        // Write header size varint
        byte[] headerSizeBytes = new byte[BinaryUtils.varIntSize(totalHeaderSize)];
        BinaryUtils.writeVarInt(headerSizeBytes, 0, totalHeaderSize);
        headerBuf.write(headerSizeBytes, 0, headerSizeBytes.length);

        // Write serial types
        for (byte[] stBytes : serialTypeVarInts) {
            headerBuf.write(stBytes, 0, stBytes.length);
        }

        // Write body (column values)
        for (SQLiteValue value : values) {
            byte[] bodyData = encodeValue(value);
            if (bodyData != null && bodyData.length > 0) {
                bodyBuf.write(bodyData, 0, bodyData.length);
            }
        }

        // Combine
        byte[] header = headerBuf.toByteArray();
        byte[] body = bodyBuf.toByteArray();
        byte[] result = new byte[header.length + body.length];
        System.arraycopy(header, 0, result, 0, header.length);
        System.arraycopy(body, 0, result, header.length, body.length);
        return result;
    }

    /**
     * Deserialize a record from byte data at the given offset.
     * Returns the Record and the total bytes consumed.
     */
    public static RecordAndSize deserialize(byte[] data, int offset) {
        // Read header size
        long[] headerSizeResult = BinaryUtils.readVarInt(data, offset);
        long headerSize = headerSizeResult[0];
        int headerStart = offset;
        int pos = offset + (int) headerSizeResult[1];

        // Read serial types
        List<Integer> serialTypes = new ArrayList<>();
        while (pos < headerStart + headerSize) {
            long[] stResult = BinaryUtils.readVarInt(data, pos);
            serialTypes.add((int) stResult[0]);
            pos += (int) stResult[1];
        }

        // Read values from body
        List<SQLiteValue> values = new ArrayList<>();
        int bodyOffset = headerStart + (int) headerSize;
        for (int serialType : serialTypes) {
            SQLiteValue value = decodeValue(data, bodyOffset, serialType);
            values.add(value);
            bodyOffset += SQLiteValue.serialTypeSize(serialType);
        }

        int totalSize = bodyOffset - offset;
        return new RecordAndSize(new Record(values), totalSize);
    }

    /**
     * Get the total serialized size of this record.
     */
    public int serializedSize() {
        return serialize().length;
    }

    private static int computeSerialType(SQLiteValue value) {
        switch (value.getType()) {
            case NULL: return 0;
            case INTEGER: {
                long v = value.asLong();
                if (v == 0) return 8;
                if (v == 1) return 9;
                boolean neg = v < 0;
                long abs = neg ? -(v + 1) : v;
                if (abs <= 0x7F) return 1;
                if (abs <= 0x7FFF) return 2;
                if (abs <= 0x7FFFFF) return 3;
                if (abs <= 0x7FFFFFFFL) return 4;
                if (abs <= 0x7FFFFFFFFFFFL) return 5;
                return 6;
            }
            case FLOAT: return 7;
            case TEXT: return value.getText().length() * 2 + 13;
            case BLOB: return value.getBlob().length * 2 + 12;
            default: return 0;
        }
    }

    private static byte[] encodeValue(SQLiteValue value) {
        switch (value.getType()) {
            case NULL: return new byte[0];
            case INTEGER: {
                long v = value.asLong();
                if (v == 0) return new byte[0];
                if (v == 1) return new byte[0];
                boolean neg = v < 0;
                long abs = neg ? -(v + 1) : v;

                if (abs <= 0x7FL) {
                    byte b = (byte) (neg ? ~abs : abs);
                    return new byte[]{b};
                }
                if (abs <= 0x7FFFL) {
                    short s = (short) (neg ? ~abs : abs);
                    return new byte[]{(byte) (s >> 8), (byte) s};
                }
                if (abs <= 0x7FFFFFL) {
                    int i = (int) abs;
                    return new byte[]{
                            (byte) (neg ? ~(i >> 16) & 0xFF : (i >> 16) & 0xFF),
                            (byte) (neg ? ~(i >> 8) & 0xFF : (i >> 8) & 0xFF),
                            (byte) (neg ? ~i & 0xFF : i & 0xFF)
                    };
                }
                if (abs <= 0x7FFFFFFFL) {
                    int i = (int) (neg ? ~abs : abs);
                    return new byte[]{
                            (byte) (i >> 24), (byte) (i >> 16),
                            (byte) (i >> 8), (byte) i
                    };
                }
                // 48-bit or 64-bit
                if (abs <= 0x7FFFFFFFFFFFL) {
                    long l = neg ? ~abs : abs;
                    return new byte[]{
                            (byte) (l >> 40), (byte) (l >> 32),
                            (byte) (l >> 24), (byte) (l >> 16),
                            (byte) (l >> 8), (byte) l
                    };
                }
                long l = neg ? ~abs : abs;
                return new byte[]{
                        (byte) (l >> 56), (byte) (l >> 48),
                        (byte) (l >> 40), (byte) (l >> 32),
                        (byte) (l >> 24), (byte) (l >> 16),
                        (byte) (l >> 8), (byte) l
                };
            }
            case FLOAT: {
                long bits = Double.doubleToRawLongBits(value.asDouble());
                byte[] result = new byte[8];
                for (int i = 0; i < 8; i++) {
                    result[i] = (byte) ((bits >> (56 - i * 8)) & 0xFF);
                }
                return result;
            }
            case TEXT:
                return value.getText().getBytes();
            case BLOB:
                return value.getBlob();
            default:
                return new byte[0];
        }
    }

    private static SQLiteValue decodeValue(byte[] data, int offset, int serialType) {
        int size = SQLiteValue.serialTypeSize(serialType);

        switch (serialType) {
            case 0: return SQLiteValue.fromNull();
            case 8: return SQLiteValue.fromLong(0);
            case 9: return SQLiteValue.fromLong(1);
            case 7: {
                long bits = 0;
                for (int i = 0; i < 8; i++) {
                    bits = (bits << 8) | (data[offset + i] & 0xFF);
                }
                return SQLiteValue.fromDouble(Double.longBitsToDouble(bits));
            }
            case 1: {
                byte b = data[offset];
                return SQLiteValue.fromLong(b); // sign-extends
            }
            case 2: {
                short s = (short) (((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF));
                return SQLiteValue.fromLong(s);
            }
            case 3: {
                int v = ((data[offset] & 0xFF) << 16) |
                        ((data[offset + 1] & 0xFF) << 8) |
                        (data[offset + 2] & 0xFF);
                // Sign extend from 24-bit
                if ((v & 0x800000) != 0) v |= 0xFF000000;
                return SQLiteValue.fromLong(v);
            }
            case 4: {
                int v = (int) BinaryUtils.readUInt32(data, offset);
                return SQLiteValue.fromLong(v);
            }
            case 5: {
                long hi = ((data[offset] & 0xFF) << 16) |
                          ((data[offset + 1] & 0xFF) << 8) |
                          (data[offset + 2] & 0xFF);
                long lo = ((data[offset + 3] & 0xFF) << 16) |
                          ((data[offset + 4] & 0xFF) << 8) |
                          (data[offset + 5] & 0xFF);
                long v = (hi << 24) | lo;
                if ((v & 0x800000000000L) != 0) v |= 0xFFFF000000000000L;
                return SQLiteValue.fromLong(v);
            }
            case 6: {
                long v = BinaryUtils.bytesToLong(data, offset);
                return SQLiteValue.fromLong(v);
            }
            default:
                if (serialType >= 12 && serialType % 2 == 0) {
                    // BLOB
                    byte[] blob = new byte[size];
                    System.arraycopy(data, offset, blob, 0, size);
                    return SQLiteValue.fromBlob(blob);
                }
                if (serialType >= 13 && serialType % 2 == 1) {
                    // TEXT
                    return SQLiteValue.fromText(new String(data, offset, size));
                }
                return SQLiteValue.fromNull();
        }
    }

    /**
     * Helper class to return both record and size from deserialization.
     */
    public static class RecordAndSize {
        public final Record record;
        public final int size;

        public RecordAndSize(Record record, int size) {
            this.record = record;
            this.size = size;
        }
    }
}
