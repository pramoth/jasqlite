package com.jasqlite.sql.ast;

import java.util.List;

public class CTEDefinition extends ASTNode {
    public String name;
    public List<String> columnNames;
    public SelectStatement select;
}
