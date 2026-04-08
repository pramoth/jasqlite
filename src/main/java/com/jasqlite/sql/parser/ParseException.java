package com.jasqlite.sql.parser;

public class ParseException extends Exception {
    public ParseException(String message) { super(message); }
    public ParseException(String message, int position) { super(message + " at position " + position); }
}
