package com.jasqlite.store;

import java.util.List;

/**
 * Represents index metadata.
 */
public class IndexInfo {
    public String name;
    public String tableName;
    public int rootPage;
    public boolean unique;
    public String sql;
    public List<String> columns;
    public List<Boolean> descending;
    public String where; // partial index condition
}
