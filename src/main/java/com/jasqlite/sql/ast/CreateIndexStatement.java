package com.jasqlite.sql.ast;

import java.util.List;

public class CreateIndexStatement extends Statement {
    public boolean unique;
    public boolean ifNotExists;
    public String indexName;
    public String tableName;
    public String databaseName;
    public List<IndexedColumn> columns;
    public Expression where;
}
