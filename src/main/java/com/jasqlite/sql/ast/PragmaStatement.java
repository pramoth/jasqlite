package com.jasqlite.sql.ast;

public class PragmaStatement extends Statement {
    public String databaseName;
    public String pragmaName;
    public Expression value;
}
