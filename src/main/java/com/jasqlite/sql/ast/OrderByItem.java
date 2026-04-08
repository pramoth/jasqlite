package com.jasqlite.sql.ast;

public class OrderByItem extends ASTNode {
    public Expression expression;
    public Enums.OrderDirection direction;
    public Enums.NullsOrdering nullsOrdering;
}
