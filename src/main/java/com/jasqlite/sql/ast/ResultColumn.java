package com.jasqlite.sql.ast;

public class ResultColumn extends ASTNode {
    public Expression expression;
    public String alias;
    public boolean isStar;
    public String starTableName;
}
