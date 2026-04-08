package com.jasqlite.sql.ast;

import java.util.List;

public class ColumnDefinition extends ASTNode {
    public String name;
    public TypeName typeName;
    public List<ColumnConstraint> constraints;
}
