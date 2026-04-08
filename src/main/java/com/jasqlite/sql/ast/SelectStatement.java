package com.jasqlite.sql.ast;

import java.util.List;

public class SelectStatement extends Statement {
    public boolean distinct;
    public List<ResultColumn> columns;
    public List<TableOrSubquery> from;
    public Expression where;
    public List<Expression> groupBy;
    public Expression having;
    public List<OrderByItem> orderBy;
    public Expression limit;
    public Expression offset;
    public List<SelectStatement> unions;
    public Enums.UnionType unionType;
}
