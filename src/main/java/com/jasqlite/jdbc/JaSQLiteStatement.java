package com.jasqlite.jdbc;

import com.jasqlite.store.Database;
import com.jasqlite.store.Result;
import com.jasqlite.util.SQLiteValue;

import java.sql.*;
import java.util.*;

/**
 * JDBC Statement implementation for JaSQLite.
 */
public class JaSQLiteStatement implements Statement {

    protected final JaSQLiteConnection conn;
    protected ResultSet currentResultSet;
    protected int updateCount;
    protected int maxRows;
    protected int queryTimeout;
    protected boolean closed;
    protected int fetchSize;
    protected int fetchDirection;
    protected int resultSetType;
    protected int resultSetConcurrency;
    protected boolean poolable;
    protected String cursorName;
    protected List<String> batchStatements;

    public JaSQLiteStatement(JaSQLiteConnection conn) {
        this.conn = conn;
        this.updateCount = -1;
        this.maxRows = 0;
        this.queryTimeout = 0;
        this.closed = false;
        this.fetchSize = 0;
        this.fetchDirection = ResultSet.FETCH_FORWARD;
        this.resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        this.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        this.batchStatements = new ArrayList<>();
    }

    protected Database getDatabase() { return conn.getDatabase(); }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        try {
            Result result = getDatabase().execute(sql);
            updateCount = -1;
            currentResultSet = new JaSQLiteResultSet(result);
            return currentResultSet;
        } catch (Exception e) {
            if (e instanceof SQLException) throw (SQLException) e;
            throw new SQLException(e.getMessage(), e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        try {
            Result result = getDatabase().execute(sql);
            updateCount = result.isQuery() ? 0 : result.getUpdateCount();
            currentResultSet = null;
            return updateCount;
        } catch (Exception e) {
            if (e instanceof SQLException) throw (SQLException) e;
            throw new SQLException(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws SQLException {
        closed = true;
        if (currentResultSet instanceof JaSQLiteResultSet) {
            ((JaSQLiteResultSet) currentResultSet).close();
        }
    }

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        this.queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        this.cursorName = name;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        try {
            Result result = getDatabase().execute(sql);
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

    @Override
    public ResultSet getResultSet() throws SQLException {
        return currentResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        if (currentResultSet != null) {
            currentResultSet.close();
            currentResultSet = null;
        }
        updateCount = -1;
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        this.fetchDirection = direction;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return fetchDirection;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return resultSetConcurrency;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return resultSetType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        batchStatements.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        batchStatements.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        int[] results = new int[batchStatements.size()];
        for (int i = 0; i < batchStatements.size(); i++) {
            try {
                results[i] = executeUpdate(batchStatements.get(i));
            } catch (SQLException e) {
                results[i] = Statement.EXECUTE_FAILED;
            }
        }
        batchStatements.clear();
        return results;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return conn;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return getMoreResults();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        long rowid = getDatabase().getLastInsertRowid();
        Result result = Result.query(new String[]{"last_insert_rowid()"},
                Collections.singletonList(new SQLiteValue[]{SQLiteValue.fromLong(rowid)}));
        return new JaSQLiteResultSet(result);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        this.poolable = poolable;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return poolable;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
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

    protected void checkClosed() throws SQLException {
        if (closed) throw new SQLException("Statement is closed");
    }
}
