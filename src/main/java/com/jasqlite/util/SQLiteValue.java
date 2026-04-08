package com.jasqlite.util;

import java.util.Arrays;

/**
 * Represents a SQLite value with its type information.
 */
public final class SQLiteValue implements Comparable<SQLiteValue> {

    public enum Type {
        NULL, INTEGER, FLOAT, TEXT, BLOB
    }

    private final Type type;
    private final Long integerValue;
    private final Double floatValue;
    private final String textValue;
    private final byte[] blobValue;

    public static final SQLiteValue NULL = new SQLiteValue(Type.NULL, null, null, null, null);

    private SQLiteValue(Type type, Long integerValue, Double floatValue, String textValue, byte[] blobValue) {
        this.type = type;
        this.integerValue = integerValue;
        this.floatValue = floatValue;
        this.textValue = textValue;
        this.blobValue = blobValue;
    }

    public static SQLiteValue fromNull() {
        return NULL;
    }

    public static SQLiteValue fromLong(long value) {
        return new SQLiteValue(Type.INTEGER, value, null, null, null);
    }

    public static SQLiteValue fromInt(int value) {
        return new SQLiteValue(Type.INTEGER, (long) value, null, null, null);
    }

    public static SQLiteValue fromDouble(double value) {
        return new SQLiteValue(Type.FLOAT, null, value, null, null);
    }

    public static SQLiteValue fromText(String value) {
        return new SQLiteValue(Type.TEXT, null, null, value, null);
    }

    public static SQLiteValue fromBlob(byte[] value) {
        return new SQLiteValue(Type.BLOB, null, null, null, value);
    }

    public static SQLiteValue fromObject(Object obj) {
        if (obj == null) return fromNull();
        if (obj instanceof SQLiteValue) return (SQLiteValue) obj;
        if (obj instanceof Long) return fromLong((Long) obj);
        if (obj instanceof Integer) return fromInt((Integer) obj);
        if (obj instanceof Short) return fromInt((Short) obj);
        if (obj instanceof Byte) return fromInt((Byte) obj);
        if (obj instanceof Double) return fromDouble((Double) obj);
        if (obj instanceof Float) return fromDouble((Float) obj);
        if (obj instanceof String) return fromText((String) obj);
        if (obj instanceof byte[]) return fromBlob((byte[]) obj);
        if (obj instanceof Boolean) return fromLong((Boolean) obj ? 1 : 0);
        return fromText(obj.toString());
    }

    public Type getType() { return type; }
    public boolean isNull() { return type == Type.NULL; }
    public boolean isInteger() { return type == Type.INTEGER; }
    public boolean isFloat() { return type == Type.FLOAT; }
    public boolean isText() { return type == Type.TEXT; }
    public boolean isBlob() { return type == Type.BLOB; }
    public boolean isNumeric() { return type == Type.INTEGER || type == Type.FLOAT; }

    public Long getInteger() { return integerValue; }
    public Double getFloat() { return floatValue; }
    public String getText() { return textValue; }
    public byte[] getBlob() { return blobValue; }

    public long asLong() {
        switch (type) {
            case INTEGER: return integerValue;
            case FLOAT: return floatValue.longValue();
            case TEXT: return Long.parseLong(textValue);
            default: return 0;
        }
    }

    public double asDouble() {
        switch (type) {
            case INTEGER: return integerValue.doubleValue();
            case FLOAT: return floatValue;
            case TEXT: return Double.parseDouble(textValue);
            default: return 0.0;
        }
    }

    public String asString() {
        switch (type) {
            case NULL: return null;
            case INTEGER: return integerValue.toString();
            case FLOAT: return floatValue.toString();
            case TEXT: return textValue;
            case BLOB: return new String(blobValue);
            default: return "";
        }
    }

    public Object asObject() {
        switch (type) {
            case NULL: return null;
            case INTEGER: return integerValue;
            case FLOAT: return floatValue;
            case TEXT: return textValue;
            case BLOB: return blobValue;
            default: return null;
        }
    }

    /**
     * Get the serial type for record format encoding.
     */
    public int serialType() {
        switch (type) {
            case NULL: return 0;
            case INTEGER: {
                long v = integerValue;
                if (v == 0) return 8;
                if (v == 1) return 9;
                if (v >= -1 && v <= 0) return 8;
                if (v > 0) {
                    if (v <= 127) return 1;
                    if (v <= 32767) return 2;
                    if (v <= 8388607) return 3;
                    if (v <= 2147483647L) return 4;
                    if (v <= 140737488355327L) return 5;
                    return 6;
                } else {
                    long abs = -v - 1;
                    if (abs < 128) return 1;
                    if (abs < 32768) return 2;
                    if (abs < 8388608) return 3;
                    if (abs < 2147483648L) return 4;
                    if (abs < 140737488355328L) return 5;
                    return 6;
                }
            }
            case FLOAT: return 7;
            case TEXT: return textValue.length() * 2 + 13;
            case BLOB: return blobValue.length * 2 + 12;
            default: return 0;
        }
    }

    /**
     * Get the content size for this value's serial type.
     */
    public int contentSize() {
        switch (type) {
            case NULL: return 0;
            case INTEGER: {
                long v = integerValue;
                if (v == 0) return 0;
                if (v == 1) return 0;
                int abs = 0;
                long val = (v < 0) ? -v - 1 : v;
                while (val != 0) { abs++; val >>= 8; }
                return Math.max(1, abs);
            }
            case FLOAT: return 8;
            case TEXT: return textValue.length();
            case BLOB: return blobValue.length;
            default: return 0;
        }
    }

    /**
     * Get the storage size for a serial type.
     */
    public static int serialTypeSize(int serialType) {
        switch (serialType) {
            case 0: return 0;  // NULL
            case 1: return 1;  // 8-bit int
            case 2: return 2;  // 16-bit int
            case 3: return 3;  // 24-bit int
            case 4: return 4;  // 32-bit int
            case 5: return 6;  // 48-bit int
            case 6: return 8;  // 64-bit int
            case 7: return 8;  // IEEE 754 float
            case 8: return 0;  // integer 0
            case 9: return 0;  // integer 1
            case 10: case 11: return 0; // reserved
            default:
                if (serialType >= 12 && serialType % 2 == 0) return (serialType - 12) / 2; // BLOB
                if (serialType >= 13 && serialType % 2 == 1) return (serialType - 13) / 2; // TEXT
                return 0;
        }
    }

    @Override
    public int compareTo(SQLiteValue other) {
        // SQLite type affinity ordering: NULL < INTEGER/FLOAT < TEXT < BLOB
        if (this.isNull() && other.isNull()) return 0;
        if (this.isNull()) return -1;
        if (other.isNull()) return 1;

        // Numeric comparison
        if (this.isNumeric() && other.isNumeric()) {
            return Double.compare(this.asDouble(), other.asDouble());
        }

        // If one is numeric and other is text/blob, numeric < text
        if (this.isNumeric() && !other.isNumeric()) return -1;
        if (!this.isNumeric() && other.isNumeric()) return 1;

        // TEXT vs TEXT
        if (this.isText() && other.isText()) {
            return this.textValue.compareTo(other.textValue);
        }

        // BLOB vs BLOB
        if (this.isBlob() && other.isBlob()) {
            int minLen = Math.min(this.blobValue.length, other.blobValue.length);
            for (int i = 0; i < minLen; i++) {
                int cmp = (this.blobValue[i] & 0xFF) - (other.blobValue[i] & 0xFF);
                if (cmp != 0) return cmp;
            }
            return this.blobValue.length - other.blobValue.length;
        }

        // TEXT < BLOB
        if (this.isText() && other.isBlob()) return -1;
        if (this.isBlob() && other.isText()) return 1;

        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SQLiteValue)) return false;
        return this.compareTo((SQLiteValue) obj) == 0;
    }

    @Override
    public int hashCode() {
        switch (type) {
            case NULL: return 0;
            case INTEGER: return integerValue.hashCode();
            case FLOAT: return floatValue.hashCode();
            case TEXT: return textValue.hashCode();
            case BLOB: return Arrays.hashCode(blobValue);
            default: return 0;
        }
    }

    @Override
    public String toString() {
        switch (type) {
            case NULL: return "NULL";
            case INTEGER: return integerValue.toString();
            case FLOAT: return floatValue.toString();
            case TEXT: return textValue;
            case BLOB: return "[BLOB " + blobValue.length + " bytes]";
            default: return "UNKNOWN";
        }
    }

    /**
     * Try to coerce this value to numeric.
     */
    public SQLiteValue coerceToNumeric() {
        if (type == Type.TEXT) {
            String t = textValue.trim();
            try {
                if (t.contains(".") || t.contains("e") || t.contains("E")) {
                    return fromDouble(Double.parseDouble(t));
                }
                return fromLong(Long.parseLong(t));
            } catch (NumberFormatException e) {
                return this;
            }
        }
        return this;
    }
}
