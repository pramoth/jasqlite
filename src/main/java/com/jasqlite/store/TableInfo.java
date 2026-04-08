package com.jasqlite.store;

import java.util.List;

/**
 * Represents table metadata.
 */
public class TableInfo {
    public String name;
    public boolean temporary;
    public int rootPage;
    public String sql;
    public List<ColumnInfo> columns;
    public String withoutRowid;
    public String strict;
    
    public ColumnInfo getColumn(String name) {
        if (columns == null) return null;
        for (ColumnInfo col : columns) {
            if (col.name.equalsIgnoreCase(name)) return col;
        }
        // Check for rowid
        if ("rowid".equalsIgnoreCase(name) || "_rowid_".equalsIgnoreCase(name) || "oid".equalsIgnoreCase(name)) {
            ColumnInfo ci = new ColumnInfo();
            ci.name = "rowid";
            ci.type = "INTEGER";
            ci.primaryKey = true;
            return ci;
        }
        return null;
    }
    
    public int getColumnIndex(String name) {
        if (columns == null) return -1;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name.equalsIgnoreCase(name)) return i;
        }
        return -1;
    }
}
