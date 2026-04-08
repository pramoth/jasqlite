package com.jasqlite.sql.ast;

import java.util.List;

public class TableOrSubquery extends ASTNode {
    public enum Type { TABLE, SUBQUERY, JOIN, FUNCTION }

    public Type type;
    public String tableName;
    public String databaseName;
    public String alias;
    public SelectStatement subquery;
    public Expressions.FunctionCall tableFunction;

    public TableOrSubquery left;
    public TableOrSubquery right;
    public Enums.JoinType joinType;
    public Expression onCondition;
    public List<String> usingColumns;

    public boolean indexed;
    public String indexName;
    public boolean notIndexed;
}
