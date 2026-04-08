package com.jasqlite.sql.ast;

import java.util.List;

public class Expressions {
    private Expressions() {}

    public static class Literal extends Expression {
        public enum Type { NULL, INTEGER, FLOAT, STRING, BLOB, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP }
        public Type type;
        public String value;
    }

    public static class Variable extends Expression {
        public String name;
    }

    public static class ColumnRef extends Expression {
        public String tableName;
        public String columnName;
        public String databaseName;
    }

    public static class RowIdRef extends Expression {
        public String tableName;
    }

    public static class Binary extends Expression {
        public Expression left;
        public BinaryOp operator;
        public Expression right;
    }

    public enum BinaryOp {
        ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO,
        CONCAT, BIT_AND, BIT_OR, LSHIFT, RSHIFT,
        EQ, NEQ, LT, LTE, GT, GTE,
        AND, OR, IS, IS_NOT,
        LIKE, GLOB, REGEXP, MATCH, IN
    }

    public static class Unary extends Expression {
        public UnaryOp operator;
        public Expression operand;
    }

    public enum UnaryOp { NEGATE, NOT, BIT_NOT }

    public static class FunctionCall extends Expression {
        public String name;
        public boolean distinct;
        public List<Expression> arguments;
        public Expression filter;
        public WindowDefinition over;
        public boolean star;
    }

    public static class Case extends Expression {
        public Expression operand;
        public List<WhenThen> whenClauses;
        public Expression elseExpression;
    }

    public static class WhenThen extends ASTNode {
        public Expression when;
        public Expression then;
    }

    public static class Cast extends Expression {
        public Expression expression;
        public TypeName typeName;
    }

    public static class Collate extends Expression {
        public Expression expression;
        public String collationName;
    }

    public static class Between extends Expression {
        public Expression expression;
        public Expression low;
        public Expression high;
        public boolean not;
    }

    public static class In extends Expression {
        public Expression expression;
        public boolean not;
        public List<Expression> values;
        public SelectStatement subquery;
        public String tableName;
    }

    public static class Exists extends Expression {
        public SelectStatement subquery;
        public boolean not;
    }

    public static class Subquery extends Expression {
        public SelectStatement subquery;
    }

    public static class IsNull extends Expression {
        public Expression expression;
        public boolean isNotNull;
    }

    public static class Raise extends Expression {
        public Enums.RaiseType type;
        public String message;
    }
}
