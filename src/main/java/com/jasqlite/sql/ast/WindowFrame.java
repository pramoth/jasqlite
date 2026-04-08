package com.jasqlite.sql.ast;

public class WindowFrame extends ASTNode {
    public enum FrameType { RANGE, ROWS, GROUPS }
    public FrameType type;
    public FrameBound start;
    public FrameBound end;
    public boolean excludeCurrentRow;
    public boolean excludeGroup;
    public boolean excludeTies;
    public boolean excludeOthers;
}
