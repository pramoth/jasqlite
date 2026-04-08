package com.jasqlite.sql.ast;

public class AttachStatement extends Statement {
    public Expression databasePath;
    public String databaseName;
    public Expression key;
}
