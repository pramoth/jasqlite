package com.jasqlite.sql.ast;

import java.util.List;

public class InsertStatement extends Statement {
    public Enums.ConflictAlgorithm conflictAlgorithm;
    public String tableName;
    public String databaseName;
    public List<String> columns;
    public SelectStatement select;
    public List<List<Expression>> values;
    public Expression returning;
    public boolean isReplace;
}
