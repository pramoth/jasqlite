package com.jasqlite.jdbc;

import com.jasqlite.store.Result;

import java.sql.*;

/**
 * ResultSetMetaData implementation for JaSQLite.
 */
public class JaSQLiteResultSetMetaData implements ResultSetMetaData {

    private final Result result;

    public JaSQLiteResultSetMetaData(Result result) {
        this.result = result;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return result.getColumnCount();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return true;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        String[] names = result.getColumnNames();
        if (names == null || column < 1 || column > names.length) return "col" + column;
        return names[column - 1];
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        if (result.getRowCount() == 0) return Types.VARCHAR;
        com.jasqlite.util.SQLiteValue val = result.getValue(0, column - 1);
        if (val == null || val.isNull()) return Types.NULL;
        switch (val.getType()) {
            case INTEGER: return Types.BIGINT;
            case FLOAT: return Types.DOUBLE;
            case TEXT: return Types.VARCHAR;
            case BLOB: return Types.VARBINARY;
            default: return Types.VARCHAR;
        }
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        if (result.getRowCount() == 0) return "TEXT";
        com.jasqlite.util.SQLiteValue val = result.getValue(0, column - 1);
        if (val == null || val.isNull()) return "NULL";
        switch (val.getType()) {
            case INTEGER: return "INTEGER";
            case FLOAT: return "REAL";
            case TEXT: return "TEXT";
            case BLOB: return "BLOB";
            default: return "TEXT";
        }
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        int type = getColumnType(column);
        switch (type) {
            case Types.BIGINT: return Long.class.getName();
            case Types.DOUBLE: return Double.class.getName();
            case Types.VARBINARY: return "[B";
            case Types.NULL: return Object.class.getName();
            default: return String.class.getName();
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
