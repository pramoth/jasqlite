package com.jasqlite.sql.ast;

import java.util.List;

public class UpdateStatement extends Statement {
    public Enums.ConflictAlgorithm conflictAlgorithm;
    public String tableName;
    public String databaseName;
    public List<ColumnAssignment> assignments;
    public Expression where;
    public List<OrderByItem> orderBy;
    public Expression limit;
    public Expression returning;
}
