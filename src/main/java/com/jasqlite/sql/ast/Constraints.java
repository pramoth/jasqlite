package com.jasqlite.sql.ast;

import java.util.List;

public class Constraints {
    private Constraints() {}

    public static class PrimaryKeyColumnConstraint extends ColumnConstraint {
        public Enums.OrderDirection order;
        public boolean autoIncrement;
    }

    public static class NotNullConstraint extends ColumnConstraint {
    }

    public static class UniqueColumnConstraint extends ColumnConstraint {
    }

    public static class CheckColumnConstraint extends ColumnConstraint {
        public Expression expression;
    }

    public static class DefaultColumnConstraint extends ColumnConstraint {
        public Expression expression;
    }

    public static class ForeignKeyColumnConstraint extends ColumnConstraint {
        public String foreignTable;
        public List<String> foreignColumns;
        public Enums.ForeignKeyAction onDelete;
        public Enums.ForeignKeyAction onUpdate;
        public Enums.ForeignKeyDeferment deferment;
    }

    public static class CollateColumnConstraint extends ColumnConstraint {
        public String collationName;
    }

    public static class GeneratedColumnConstraint extends ColumnConstraint {
        public Expression expression;
        public boolean stored;
    }
}
