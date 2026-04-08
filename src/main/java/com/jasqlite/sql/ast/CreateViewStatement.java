package com.jasqlite.sql.ast;

import java.util.List;

public class CreateViewStatement extends Statement {
    public boolean temporary;
    public boolean ifNotExists;
    public String viewName;
    public String databaseName;
    public List<String> columns;
    public SelectStatement select;
}
