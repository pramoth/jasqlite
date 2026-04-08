package com.jasqlite.sql.ast;

public class DropStatements {
    private DropStatements() {}

    public static class DropTable extends Statement {
        public boolean ifExists;
        public String tableName;
        public String databaseName;
    }

    public static class DropIndex extends Statement {
        public boolean ifExists;
        public String indexName;
        public String databaseName;
    }

    public static class DropView extends Statement {
        public boolean ifExists;
        public String viewName;
        public String databaseName;
    }

    public static class DropTrigger extends Statement {
        public boolean ifExists;
        public String triggerName;
        public String databaseName;
    }
}
