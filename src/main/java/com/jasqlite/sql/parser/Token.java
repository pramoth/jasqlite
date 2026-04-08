package com.jasqlite.sql.parser;

/**
 * A token from the SQL lexer.
 */
public class Token {
    public final TokenType type;
    public final String value;
    public final int position;

    public Token(TokenType type, String value, int position) {
        this.type = type;
        this.value = value;
        this.position = position;
    }

    @Override
    public String toString() {
        return type + "(" + value + ")";
    }

    public boolean isKeyword() {
        return type.ordinal() >= TokenType.ABORT.ordinal() &&
               type.ordinal() <= TokenType.WITHOUT.ordinal();
    }
}
