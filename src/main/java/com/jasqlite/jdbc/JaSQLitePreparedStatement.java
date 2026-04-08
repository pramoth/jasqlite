package com.jasqlite.jdbc;

import com.jasqlite.store.Database;
import com.jasqlite.store.Result;
import com.jasqlite.util.SQLiteValue;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.Calendar;

/**
 * JDBC PreparedStatement implementation for JaSQLite.
 */
public class JaSQLitePreparedStatement extends JaSQLiteStatement implements PreparedStatement {

    private final String sql;
    private final Map<Integer, SQLiteValue> parameters;

    public JaSQLitePreparedStatement(JaSQLiteConnection conn, String sql) {
        super(conn);
        this.sql = sql;
        this.parameters = new HashMap<>();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkClosed();
        try {
            SQLiteValue[] params = buildParamArray();
            Result result = getDatabase().execute(sql, params);
            updateCount = -1;
            currentResultSet = new JaSQLiteResultSet(result);
            return currentResultSet;
        } catch (Exception e) {
            if (e instanceof SQLException) throw (SQLException) e;
            throw new SQLException(e.getMessage(), e);
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkClosed();
        try {
            SQLiteValue[] params = buildParamArray();
            Result result = getDatabase().execute(sql, params);
            updateCount = result.isQuery() ? 0 : result.getUpdateCount();
            currentResultSet = null;
            return updateCount;
        } catch (Exception e) {
            if (e instanceof SQLException) throw (SQLException) e;
            throw new SQLException(e.getMessage(), e);
        }
    }

    @Override
    public boolean execute() throws SQLException {
        checkClosed();
        try {
            SQLiteValue[] params = buildParamArray();
            Result result = getDatabase().execute(sql, params);
            if (result.isQuery()) {
                currentResultSet = new JaSQLiteResultSet(result);
                updateCount = -1;
                return true;
            } else {
                currentResultSet = null;
                updateCount = result.getUpdateCount();
                return false;
            }
        } catch (Exception e) {
            if (e instanceof SQLException) throw (SQLException) e;
            throw new SQLException(e.getMessage(), e);
        }
    }

    private SQLiteValue[] buildParamArray() {
        if (parameters.isEmpty()) return null;
        int maxIdx = 0;
        for (int idx : parameters.keySet()) {
            if (idx > maxIdx) maxIdx = idx;
        }
        // Convert from JDBC 1-based indexing to 0-based array
        SQLiteValue[] params = new SQLiteValue[maxIdx];
        for (Map.Entry<Integer, SQLiteValue> entry : parameters.entrySet()) {
            int idx = entry.getKey() - 1; // Convert 1-based to 0-based
            if (idx >= 0 && idx < params.length) {
                params[idx] = entry.getValue();
            }
        }
        return params;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        parameters.put(parameterIndex, SQLiteValue.fromNull());
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        parameters.put(parameterIndex, SQLiteValue.fromLong(x ? 1 : 0));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        parameters.put(parameterIndex, SQLiteValue.fromLong(x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        parameters.put(parameterIndex, SQLiteValue.fromLong(x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        parameters.put(parameterIndex, SQLiteValue.fromLong(x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        parameters.put(parameterIndex, SQLiteValue.fromLong(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        parameters.put(parameterIndex, SQLiteValue.fromDouble(x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        parameters.put(parameterIndex, SQLiteValue.fromDouble(x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        if (x == null) setNull(parameterIndex, Types.NUMERIC);
        else parameters.put(parameterIndex, SQLiteValue.fromText(x.toPlainString()));
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        if (x == null) setNull(parameterIndex, Types.VARCHAR);
        else parameters.put(parameterIndex, SQLiteValue.fromText(x));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        if (x == null) setNull(parameterIndex, Types.VARBINARY);
        else parameters.put(parameterIndex, SQLiteValue.fromBlob(x));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (x == null) setNull(parameterIndex, Types.DATE);
        else parameters.put(parameterIndex, SQLiteValue.fromText(x.toString()));
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (x == null) setNull(parameterIndex, Types.TIME);
        else parameters.put(parameterIndex, SQLiteValue.fromText(x.toString()));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        if (x == null) setNull(parameterIndex, Types.TIMESTAMP);
        else parameters.put(parameterIndex, SQLiteValue.fromText(x.toString()));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearParameters() throws SQLException {
        parameters.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) setNull(parameterIndex, Types.NULL);
        else parameters.put(parameterIndex, SQLiteValue.fromObject(x));
    }

    @Override
    public void addBatch() throws SQLException {
        super.addBatch(sql);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String x) throws SQLException {
        setString(parameterIndex, x);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
