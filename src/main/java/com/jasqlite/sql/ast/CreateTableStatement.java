package com.jasqlite.sql.ast;

import java.util.List;

public class CreateTableStatement extends Statement {
    public boolean temporary;
    public boolean ifNotExists;
    public String tableName;
    public String databaseName;
    public List<ColumnDefinition> columns;
    public List<TableConstraint> constraints;
    public SelectStatement asSelect;
}
