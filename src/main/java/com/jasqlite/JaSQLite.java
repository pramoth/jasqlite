package com.jasqlite;

import com.jasqlite.jdbc.JaSQLiteDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Main entry point for JaSQLite - a SQLite-compatible SQL engine in Java.
 * Binary compatible with SQLite database files.
 */
public class JaSQLite {

    public static final String DRIVER_PREFIX = "jdbc:jasqlite:";
    public static final String SQLITE_HEADER_STRING = "SQLite format 3\u0000";
    public static final int SQLITE_VERSION_NUMBER = 3046001; // 3.46.1
    public static final int DEFAULT_PAGE_SIZE = 4096;
    public static final int MAX_PAGE_SIZE = 65536;
    public static final int MIN_PAGE_SIZE = 512;
    public static final int DEFAULT_CACHE_SIZE = 2000;
    public static final int MAX_PAYLOAD_BYTES = 1000000000;

    static {
        try {
            DriverManager.registerDriver(new JaSQLiteDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register JaSQLite driver", e);
        }
    }

    public static Connection createConnection(String url) throws SQLException {
        if (!url.startsWith(DRIVER_PREFIX)) {
            throw new SQLException("Invalid URL: must start with " + DRIVER_PREFIX);
        }
        String path = url.substring(DRIVER_PREFIX.length());
        return createConnection(path, false);
    }

    public static Connection createConnection(String path, boolean readOnly) throws SQLException {
        com.jasqlite.store.Database db = new com.jasqlite.store.Database(path, readOnly);
        return new com.jasqlite.jdbc.JaSQLiteConnection(db);
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("JaSQLite - SQLite-compatible SQL Engine in Java");
            System.out.println("Usage: java -jar jasqlite.jar <database-file>");
            System.out.println("       java -jar jasqlite.jar --version");
            return;
        }
        if ("--version".equals(args[0])) {
            System.out.println("JaSQLite 1.0.0 (compatible with SQLite 3.46.1)");
            return;
        }
        String dbPath = args[0];
        try {
            Connection conn = createConnection(dbPath);
            // Simple shell mode
            java.util.Scanner scanner = new java.util.Scanner(System.in);
            System.out.println("JaSQLite version 1.0.0");
            System.out.println("Enter \".help\" for usage hints.");
            while (true) {
                System.out.print("jasqlite> ");
                String line = scanner.nextLine().trim();
                if (line.isEmpty()) continue;
                if (".quit".equals(line) || ".exit".equals(line)) break;
                if (".help".equals(line)) {
                    System.out.println(".quit    Exit this program");
                    System.out.println(".exit    Exit this program");
                    System.out.println(".help    Show this message");
                    System.out.println(".tables  List tables");
                    System.out.println(".schema  Show schema");
                    continue;
                }
                if (".tables".equals(line)) {
                    try (Statement stmt = conn.createStatement();
                         java.sql.ResultSet rs = stmt.executeQuery(
                                 "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")) {
                        while (rs.next()) {
                            System.out.println(rs.getString(1));
                        }
                    }
                    continue;
                }
                if (".schema".equals(line)) {
                    try (Statement stmt = conn.createStatement();
                         java.sql.ResultSet rs = stmt.executeQuery(
                                 "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name")) {
                        while (rs.next()) {
                            System.out.println(rs.getString(1) + ";");
                        }
                    }
                    continue;
                }
                try (Statement stmt = conn.createStatement()) {
                    if (stmt.execute(line)) {
                        java.sql.ResultSet rs = stmt.getResultSet();
                        int cols = rs.getMetaData().getColumnCount();
                        // Print column headers
                        StringBuilder header = new StringBuilder();
                        StringBuilder separator = new StringBuilder();
                        for (int i = 1; i <= cols; i++) {
                            if (i > 1) { header.append("|"); separator.append("-"); }
                            String name = rs.getMetaData().getColumnName(i);
                            header.append(name);
                            for (int j = 0; j < name.length(); j++) separator.append("-");
                        }
                        System.out.println(header);
                        System.out.println(separator);
                        while (rs.next()) {
                            StringBuilder row = new StringBuilder();
                            for (int i = 1; i <= cols; i++) {
                                if (i > 1) row.append("|");
                                row.append(rs.getString(i) != null ? rs.getString(i) : "NULL");
                            }
                            System.out.println(row);
                        }
                    } else {
                        System.out.println("Changes: " + stmt.getUpdateCount());
                    }
                } catch (SQLException e) {
                    System.out.println("Error: " + e.getMessage());
                }
            }
            conn.close();
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
