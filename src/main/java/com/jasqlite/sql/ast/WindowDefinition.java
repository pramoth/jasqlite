package com.jasqlite.sql.ast;

import java.util.List;

public class WindowDefinition extends ASTNode {
    public String name;
    public List<Expression> partitionBy;
    public List<OrderByItem> orderBy;
    public WindowFrame frame;
}
