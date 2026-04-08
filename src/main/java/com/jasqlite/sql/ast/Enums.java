package com.jasqlite.sql.ast;

public class Enums {
    private Enums() {}
    public enum ConflictAlgorithm { NONE, ROLLBACK, ABORT, FAIL, IGNORE, REPLACE }
    public enum ForeignKeyAction { NO_ACTION, RESTRICT, SET_NULL, SET_DEFAULT, CASCADE }
    public enum ForeignKeyDeferment { NONE, DEFERRABLE_INITIALLY_DEFERRED, DEFERRABLE_INITIALLY_IMMEDIATE, NOT_DEFERRABLE }
    public enum OrderDirection { ASC, DESC }
    public enum NullsOrdering { FIRST, LAST, NONE }
    public enum JoinType { INNER, LEFT, RIGHT, FULL, CROSS, NATURAL_INNER, NATURAL_LEFT, NATURAL_RIGHT, NATURAL_FULL }
    public enum UnionType { NONE, UNION, UNION_ALL, INTERSECT, EXCEPT }
    public enum TriggerTime { BEFORE, AFTER, INSTEAD_OF }
    public enum TriggerEvent { DELETE, INSERT, UPDATE }
    public enum TransactionType { DEFERRED, IMMEDIATE, EXCLUSIVE }
    public enum AlterType { RENAME_TO, RENAME_COLUMN, ADD_COLUMN, DROP_COLUMN }
    public enum RaiseType { IGNORE, ROLLBACK, ABORT, FAIL }
}
