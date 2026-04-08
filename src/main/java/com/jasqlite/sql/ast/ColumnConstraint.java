package com.jasqlite.sql.ast;

public abstract class ColumnConstraint extends ASTNode {
    public String name;
    public Enums.ConflictAlgorithm onConflict;
}
