package com.jasqlite.store;

/**
 * Represents column metadata.
 */
public class ColumnInfo {
    public String name;
    public String type;
    public boolean notNull;
    public String defaultValue;
    public boolean primaryKey;
    public boolean autoIncrement;
    public boolean unique;
    public String collation;
    public int cid; // column index
}
