package com.jasqlite.sql.ast;

import java.util.List;

public abstract class TableConstraint extends ASTNode {
    public String name;

    public static class PrimaryKey extends TableConstraint {
        public List<IndexedColumn> columns;
        public boolean autoIncrement;
    }

    public static class Unique extends TableConstraint {
        public List<IndexedColumn> columns;
        public Enums.ConflictAlgorithm onConflict;
    }

    public static class Check extends TableConstraint {
        public Expression expression;
    }

    public static class ForeignKey extends TableConstraint {
        public List<String> columns;
        public String foreignTable;
        public List<String> foreignColumns;
        public Enums.ForeignKeyAction onDelete;
        public Enums.ForeignKeyAction onUpdate;
        public Enums.ForeignKeyDeferment deferment;
    }
}
