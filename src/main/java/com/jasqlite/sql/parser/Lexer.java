package com.jasqlite.sql.parser;

import java.util.HashMap;
import java.util.Map;

/**
 * SQL Lexer - tokenizes SQL input into a stream of tokens.
 */
public class Lexer {

    private static final Map<String, TokenType> KEYWORDS = new HashMap<>();

    static {
        TokenType[] kws = {
            TokenType.ABORT, TokenType.ACTION, TokenType.ADD, TokenType.AFTER,
            TokenType.ALL, TokenType.ALTER, TokenType.ALWAYS, TokenType.ANALYZE,
            TokenType.AND, TokenType.AS, TokenType.ASC, TokenType.ATTACH,
            TokenType.AUTOINCREMENT, TokenType.BEFORE, TokenType.BEGIN,
            TokenType.BETWEEN, TokenType.BY, TokenType.CASCADE, TokenType.CASE,
            TokenType.CAST, TokenType.CHECK, TokenType.COLLATE, TokenType.COLUMN,
            TokenType.COMMIT, TokenType.CONFLICT, TokenType.CONSTRAINT,
            TokenType.CREATE, TokenType.CROSS, TokenType.CURRENT,
            TokenType.CURRENT_DATE, TokenType.CURRENT_TIME, TokenType.CURRENT_TIMESTAMP,
            TokenType.DATABASE, TokenType.DEFAULT, TokenType.DEFERRABLE,
            TokenType.DEFERRED, TokenType.DELETE, TokenType.DESC, TokenType.DETACH,
            TokenType.DISTINCT, TokenType.DO, TokenType.DROP, TokenType.EACH,
            TokenType.ELSE, TokenType.END, TokenType.ESCAPE, TokenType.EXCEPT,
            TokenType.EXCLUDE, TokenType.EXCLUSIVE, TokenType.EXISTS,
            TokenType.EXPLAIN, TokenType.FAIL, TokenType.FILTER, TokenType.FIRST,
            TokenType.FOLLOWING, TokenType.FOR, TokenType.FOREIGN, TokenType.FROM,
            TokenType.FULL, TokenType.GENERATED, TokenType.GLOB, TokenType.GROUP,
            TokenType.GROUPS, TokenType.HAVING, TokenType.IF, TokenType.IGNORE,
            TokenType.IMMEDIATE, TokenType.IN, TokenType.INDEX, TokenType.INDEXED,
            TokenType.INITIALLY, TokenType.INNER, TokenType.INSERT, TokenType.INSTEAD,
            TokenType.INTERSECT, TokenType.INTO, TokenType.IS, TokenType.ISNULL,
            TokenType.JOIN, TokenType.KEY, TokenType.LAST, TokenType.LEFT,
            TokenType.LIKE, TokenType.LIMIT, TokenType.MATCH, TokenType.MATERIALIZED,
            TokenType.NATURAL, TokenType.NO, TokenType.NOT, TokenType.NOTHING,
            TokenType.NOTNULL, TokenType.NULL, TokenType.NULLS, TokenType.OF,
            TokenType.OFFSET, TokenType.ON, TokenType.OR, TokenType.ORDER,
            TokenType.OTHERS, TokenType.OUTER, TokenType.OVER, TokenType.PARTITION,
            TokenType.PLAN, TokenType.PRAGMA, TokenType.PRECEDING, TokenType.PRIMARY,
            TokenType.QUERY, TokenType.RAISE, TokenType.RANGE, TokenType.RECURSIVE,
            TokenType.REFERENCES, TokenType.REGEXP, TokenType.REINDEX,
            TokenType.RELEASE, TokenType.RENAME, TokenType.REPLACE,
            TokenType.RESTRICT, TokenType.RETURNING, TokenType.RIGHT,
            TokenType.ROLLBACK, TokenType.ROW, TokenType.ROWS, TokenType.SAVEPOINT,
            TokenType.SELECT, TokenType.SET, TokenType.TABLE, TokenType.TEMP,
            TokenType.TEMPORARY, TokenType.THEN, TokenType.TIES, TokenType.TO,
            TokenType.TRANSACTION, TokenType.TRIGGER, TokenType.UNBOUNDED,
            TokenType.UNION, TokenType.UNIQUE, TokenType.UPDATE, TokenType.USING,
            TokenType.VACUUM, TokenType.VALUES, TokenType.VIEW, TokenType.VIRTUAL,
            TokenType.WHEN, TokenType.WHERE, TokenType.WINDOW, TokenType.WITH,
            TokenType.WITHOUT
        };
        for (TokenType kw : kws) {
            KEYWORDS.put(kw.name(), kw);
        }
    }

    private final String input;
    private int pos;
    private int tokenStart;

    public Lexer(String input) {
        this.input = input;
        this.pos = 0;
    }

    public Token nextToken() {
        skipWhitespace();
        if (pos >= input.length()) {
            return new Token(TokenType.EOF, "", pos);
        }

        tokenStart = pos;
        char c = input.charAt(pos);

        // Single-line comment
        if (c == '-' && pos + 1 < input.length() && input.charAt(pos + 1) == '-') {
            while (pos < input.length() && input.charAt(pos) != '\n') pos++;
            return nextToken();
        }

        // Multi-line comment
        if (c == '/' && pos + 1 < input.length() && input.charAt(pos + 1) == '*') {
            pos += 2;
            while (pos + 1 < input.length() && !(input.charAt(pos) == '*' && input.charAt(pos + 1) == '/')) pos++;
            pos += 2;
            return nextToken();
        }

        // String literal
        if (c == '\'') {
            return scanString();
        }

        // Double-quoted identifier
        if (c == '"') {
            return scanDoubleQuotedIdentifier();
        }

        // Backtick-quoted identifier
        if (c == '`') {
            return scanBacktickIdentifier();
        }

        // Square bracket identifier
        if (c == '[') {
            return scanBracketIdentifier();
        }

        // Blob literal: x'...' or X'...'
        if ((c == 'x' || c == 'X') && pos + 1 < input.length() && input.charAt(pos + 1) == '\'') {
            return scanBlobLiteral();
        }

        // Number
        if (Character.isDigit(c)) {
            return scanNumber();
        }

        // Variable (for bindings)
        if (c == '?' || c == '$' || c == '@' || c == ':' || c == '#') {
            pos++;
            if (c == '?' && pos < input.length() && Character.isDigit(input.charAt(pos))) {
                int start = pos;
                while (pos < input.length() && Character.isDigit(input.charAt(pos))) pos++;
                return new Token(TokenType.IDENTIFIER, input.substring(tokenStart, pos), tokenStart);
            }
            if (pos < input.length() && (Character.isLetter(input.charAt(pos)) || input.charAt(pos) == '_')) {
                int start = pos;
                while (pos < input.length() && (Character.isLetterOrDigit(input.charAt(pos)) || input.charAt(pos) == '_')) pos++;
                return new Token(TokenType.IDENTIFIER, input.substring(tokenStart, pos), tokenStart);
            }
            return new Token(TokenType.IDENTIFIER, input.substring(tokenStart, pos), tokenStart);
        }

        // Identifier or keyword
        if (Character.isLetter(c) || c == '_') {
            return scanIdentifier();
        }

        // Operators and punctuation
        pos++;
        switch (c) {
            case '(': return new Token(TokenType.LPAREN, "(", tokenStart);
            case ')': return new Token(TokenType.RPAREN, ")", tokenStart);
            case ',': return new Token(TokenType.COMMA, ",", tokenStart);
            case '.': return new Token(TokenType.DOT, ".", tokenStart);
            case ';': return new Token(TokenType.SEMICOLON, ";", tokenStart);
            case '*': return new Token(TokenType.STAR, "*", tokenStart);
            case '+': return new Token(TokenType.PLUS, "+", tokenStart);
            case '-': return new Token(TokenType.MINUS, "-", tokenStart);
            case '/': return new Token(TokenType.SLASH, "/", tokenStart);
            case '%': return new Token(TokenType.PERCENT, "%", tokenStart);
            case '&': return new Token(TokenType.AMPERSAND, "&", tokenStart);
            case '~': return new Token(TokenType.TILDE, "~", tokenStart);
            case '=': return new Token(TokenType.EQ, "=", tokenStart);
            case '<':
                if (pos < input.length()) {
                    if (input.charAt(pos) == '<') { pos++; return new Token(TokenType.LSHIFT, "<<", tokenStart); }
                    if (input.charAt(pos) == '=') { pos++; return new Token(TokenType.LTE, "<=", tokenStart); }
                    if (input.charAt(pos) == '>') { pos++; return new Token(TokenType.NEQ, "<>", tokenStart); }
                }
                return new Token(TokenType.LT, "<", tokenStart);
            case '>':
                if (pos < input.length()) {
                    if (input.charAt(pos) == '>') { pos++; return new Token(TokenType.RSHIFT, ">>", tokenStart); }
                    if (input.charAt(pos) == '=') { pos++; return new Token(TokenType.GTE, ">=", tokenStart); }
                }
                return new Token(TokenType.GT, ">", tokenStart);
            case '!':
                if (pos < input.length() && input.charAt(pos) == '=') {
                    pos++;
                    return new Token(TokenType.NEQ, "!=", tokenStart);
                }
                return new Token(TokenType.IDENTIFIER, "!", tokenStart);
            case '|':
                if (pos < input.length() && input.charAt(pos) == '|') {
                    pos++;
                    return new Token(TokenType.CONCAT, "||", tokenStart);
                }
                return new Token(TokenType.PIPE, "|", tokenStart);
            default:
                return new Token(TokenType.IDENTIFIER, String.valueOf(c), tokenStart);
        }
    }

    private Token scanString() {
        pos++; // skip opening quote
        StringBuilder sb = new StringBuilder();
        while (pos < input.length()) {
            char c = input.charAt(pos);
            if (c == '\'') {
                pos++;
                // Check for escaped quote ''
                if (pos < input.length() && input.charAt(pos) == '\'') {
                    sb.append('\'');
                    pos++;
                } else {
                    break;
                }
            } else {
                sb.append(c);
                pos++;
            }
        }
        return new Token(TokenType.STRING_LITERAL, sb.toString(), tokenStart);
    }

    private Token scanNumber() {
        boolean isFloat = false;
        int start = pos;
        while (pos < input.length() && Character.isDigit(input.charAt(pos))) pos++;
        if (pos < input.length() && input.charAt(pos) == '.') {
            isFloat = true;
            pos++;
            while (pos < input.length() && Character.isDigit(input.charAt(pos))) pos++;
        }
        if (pos < input.length() && (input.charAt(pos) == 'e' || input.charAt(pos) == 'E')) {
            isFloat = true;
            pos++;
            if (pos < input.length() && (input.charAt(pos) == '+' || input.charAt(pos) == '-')) pos++;
            while (pos < input.length() && Character.isDigit(input.charAt(pos))) pos++;
        }
        // Check for hexadecimal
        if (!isFloat && pos - start >= 2 && input.charAt(start) == '0' &&
            (input.charAt(start + 1) == 'x' || input.charAt(start + 1) == 'X')) {
            while (pos < input.length() && isHexDigit(input.charAt(pos))) pos++;
            return new Token(TokenType.INTEGER_LITERAL, input.substring(start, pos), tokenStart);
        }
        String value = input.substring(start, pos);
        if (isFloat) {
            return new Token(TokenType.FLOAT_LITERAL, value, tokenStart);
        }
        return new Token(TokenType.INTEGER_LITERAL, value, tokenStart);
    }

    private boolean isHexDigit(char c) {
        return Character.isDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }

    private Token scanIdentifier() {
        int start = pos;
        while (pos < input.length() && (Character.isLetterOrDigit(input.charAt(pos)) || input.charAt(pos) == '_')) {
            pos++;
        }
        String word = input.substring(start, pos);
        TokenType kwType = KEYWORDS.get(word.toUpperCase());
        if (kwType != null) {
            return new Token(kwType, word, tokenStart);
        }
        return new Token(TokenType.IDENTIFIER, word, tokenStart);
    }

    private Token scanDoubleQuotedIdentifier() {
        pos++; // skip opening "
        StringBuilder sb = new StringBuilder();
        while (pos < input.length() && input.charAt(pos) != '"') {
            sb.append(input.charAt(pos));
            pos++;
        }
        if (pos < input.length()) pos++; // skip closing "
        return new Token(TokenType.IDENTIFIER, sb.toString(), tokenStart);
    }

    private Token scanBacktickIdentifier() {
        pos++; // skip opening `
        StringBuilder sb = new StringBuilder();
        while (pos < input.length() && input.charAt(pos) != '`') {
            sb.append(input.charAt(pos));
            pos++;
        }
        if (pos < input.length()) pos++;
        return new Token(TokenType.IDENTIFIER, sb.toString(), tokenStart);
    }

    private Token scanBracketIdentifier() {
        pos++; // skip opening [
        StringBuilder sb = new StringBuilder();
        while (pos < input.length() && input.charAt(pos) != ']') {
            sb.append(input.charAt(pos));
            pos++;
        }
        if (pos < input.length()) pos++;
        return new Token(TokenType.IDENTIFIER, sb.toString(), tokenStart);
    }

    private Token scanBlobLiteral() {
        pos += 2; // skip x'
        StringBuilder sb = new StringBuilder();
        while (pos < input.length() && input.charAt(pos) != '\'') {
            sb.append(input.charAt(pos));
            pos++;
        }
        if (pos < input.length()) pos++; // skip closing '
        return new Token(TokenType.BLOB_LITERAL, sb.toString(), tokenStart);
    }

    private void skipWhitespace() {
        while (pos < input.length() && Character.isWhitespace(input.charAt(pos))) {
            pos++;
        }
    }

    /**
     * Tokenize the entire input.
     */
    public java.util.List<Token> tokenize() {
        java.util.List<Token> tokens = new java.util.ArrayList<>();
        Token token;
        do {
            token = nextToken();
            tokens.add(token);
        } while (token.type != TokenType.EOF);
        return tokens;
    }
}
