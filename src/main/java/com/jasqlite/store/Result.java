package com.jasqlite.store;

import com.jasqlite.util.SQLiteValue;

import java.util.List;

/**
 * Represents the result of executing a SQL statement.
 */
public class Result {
    public static final Result EMPTY = new Result();

    private String[] columnNames;
    private SQLiteValue[][] rows;
    private int updateCount;
    private long lastInsertRowid;
    private boolean isQuery;

    public Result() {
        this.isQuery = false;
        this.updateCount = 0;
    }

    public Result(String[] columnNames, List<SQLiteValue[]> rows) {
        this.columnNames = columnNames;
        this.rows = rows.toArray(new SQLiteValue[0][]);
        this.isQuery = true;
    }

    public static Result query(String[] columnNames, List<SQLiteValue[]> rows) {
        return new Result(columnNames, rows);
    }

    public static Result update(int count) {
        Result r = new Result();
        r.updateCount = count;
        return r;
    }

    public static Result insert(int count, long rowid) {
        Result r = new Result();
        r.updateCount = count;
        r.lastInsertRowid = rowid;
        return r;
    }

    public String[] getColumnNames() { return columnNames; }
    public SQLiteValue[][] getRows() { return rows; }
    public int getUpdateCount() { return updateCount; }
    public long getLastInsertRowid() { return lastInsertRowid; }
    public boolean isQuery() { return isQuery; }

    public int getRowCount() { return rows != null ? rows.length : 0; }
    public int getColumnCount() { return columnNames != null ? columnNames.length : 0; }

    public SQLiteValue getValue(int row, int col) {
        if (rows == null || row < 0 || row >= rows.length) return null;
        if (col < 0 || col >= rows[row].length) return null;
        return rows[row][col];
    }
}
