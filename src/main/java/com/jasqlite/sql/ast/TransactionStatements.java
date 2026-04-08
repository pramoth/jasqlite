package com.jasqlite.sql.ast;

public class TransactionStatements {
    private TransactionStatements() {}

    public static class Begin extends Statement {
        public Enums.TransactionType transactionType;
    }

    public static class Commit extends Statement {
    }

    public static class Rollback extends Statement {
        public String savepointName;
    }

    public static class Savepoint extends Statement {
        public String savepointName;
        public boolean isRelease;
    }
}
