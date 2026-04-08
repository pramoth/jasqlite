package com.jasqlite.store;

/**
 * Represents an entry in the sqlite_master table.
 */
public class SchemaEntry {
    public String type;       // 'table', 'index', 'view', 'trigger'
    public String name;
    public String tblName;
    public int rootpage;
    public String sql;
    
    public SchemaEntry() {}
    
    public SchemaEntry(String type, String name, String tblName, int rootpage, String sql) {
        this.type = type;
        this.name = name;
        this.tblName = tblName;
        this.rootpage = rootpage;
        this.sql = sql;
    }
    
    @Override
    public String toString() {
        return "SchemaEntry{type='" + type + "', name='" + name + "', tblName='" + tblName +
               "', rootpage=" + rootpage + ", sql='" + sql + "'}";
    }
}
