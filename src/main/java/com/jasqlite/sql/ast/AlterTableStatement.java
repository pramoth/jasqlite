package com.jasqlite.sql.ast;

public class AlterTableStatement extends Statement {
    public String tableName;
    public String databaseName;
    public Enums.AlterType alterType;
    public String newTableName;
    public ColumnDefinition addColumn;
    public String dropColumnName;
    public String renameColumnOld;
    public String renameColumnNew;
}
