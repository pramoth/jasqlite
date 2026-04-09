package com.jasqlite.store;

import com.jasqlite.sql.ast.Expression;
import java.util.List;

public class IndexInfo {
    public String name;
    public String tableName;
    public int rootPage;
    public boolean unique;
    public String sql;
    public List<String> columns;
    public List<Expression> columnExpressions;
    public List<Boolean> descending;
    public String where;
    public Expression whereExpr;
}
