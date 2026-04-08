package com.jasqlite.sql.ast;

public class IndexedColumn extends ASTNode {
    public Expression expression;
    public String collationName;
    public Enums.OrderDirection order;
}
