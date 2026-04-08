package com.jasqlite.sql.ast;

public class FrameBound extends ASTNode {
    public enum Type { UNBOUNDED_PRECEDING, PRECEDING, CURRENT_ROW, FOLLOWING, UNBOUNDED_FOLLOWING }
    public Type type;
    public Expression offset;
}
