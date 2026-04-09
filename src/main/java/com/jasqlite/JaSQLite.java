package com.jasqlite;

import com.jasqlite.jdbc.JaSQLiteDriver;

import java.io.*;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class JaSQLite {

    public static final String DRIVER_PREFIX = "jdbc:jasqlite:";
    public static final String SQLITE_HEADER_STRING = "SQLite format 3\u0000";
    public static final int SQLITE_VERSION_NUMBER = 3046001;
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
        String origStty = null;
        try {
            if (System.console() != null) {
                origStty = stty("-g");
                sttyRaw();
            }
            List<String> history = loadHistory();
            Connection conn = createConnection(dbPath);
            System.out.println("JaSQLite version 1.0.0");
            System.out.println("Enter \".help\" for usage hints.");
            InputStream in = System.in;
            StringBuilder lineBuf = new StringBuilder();
            while (true) {
                System.out.print(PROMPT);
                System.out.flush();
                lineBuf.setLength(0);
                if (!readLine(in, lineBuf, history)) break;
                String line = lineBuf.toString().trim();
                if (line.isEmpty()) continue;
                history.add(line);
                if (".quit".equals(line) || ".exit".equals(line)) break;
                if (".help".equals(line)) {
                    System.out.println(".quit    Exit this program");
                    System.out.println(".exit    Exit this program");
                    System.out.println(".help    Show this message");
                    System.out.println(".tables  List tables");
                    System.out.println(".schema  Show schema");
                    System.out.println(".history Show command history");
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
                if (".history".equals(line)) {
                    int start = Math.max(0, history.size() - 50);
                    for (int i = start; i < history.size(); i++) {
                        System.out.println("  " + (i + 1) + "  " + history.get(i));
                    }
                    continue;
                }
                try (Statement stmt = conn.createStatement()) {
                    if (stmt.execute(line)) {
                        java.sql.ResultSet rs = stmt.getResultSet();
                        int cols = rs.getMetaData().getColumnCount();
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
            saveHistory(history);
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
        } finally {
            if (origStty != null) stty(origStty);
        }
    }

    private static final String PROMPT = "jasqlite> ";
    private static final int PROMPT_LEN = PROMPT.length();
    private static final Path HISTORY_FILE = Paths.get(System.getProperty("user.home"), ".sql_history");
    private static final int MAX_HISTORY = 1000;

    private static List<String> loadHistory() {
        List<String> history = new ArrayList<>();
        try {
            if (Files.exists(HISTORY_FILE)) {
                List<String> lines = Files.readAllLines(HISTORY_FILE);
                for (String l : lines) {
                    if (!l.isEmpty()) history.add(l);
                }
            }
        } catch (IOException ignored) {
        }
        return history;
    }

    private static void saveHistory(List<String> history) {
        try {
            int start = Math.max(0, history.size() - MAX_HISTORY);
            List<String> toSave = history.subList(start, history.size());
            Files.write(HISTORY_FILE, toSave);
        } catch (IOException ignored) {
        }
    }

    private static boolean readLine(InputStream in, StringBuilder buf, List<String> history) {
        try {
            int cursor = 0;
            int histIdx = history.size();
            String saved = "";
            while (true) {
                int ch = in.read();
                if (ch == -1) return false;
                if (ch == '\n' || ch == '\r') {
                    System.out.println();
                    return true;
                }
                if (ch == 3) {
                    System.out.println("^C");
                    buf.setLength(0);
                    return true;
                }
                if (ch == 4) {
                    if (cursor == 0 && buf.length() == 0) return false;
                    if (cursor < buf.length()) {
                        buf.deleteCharAt(cursor);
                        redraw(buf, cursor);
                    }
                    continue;
                }
                if (ch == 127 || ch == 8) {
                    if (cursor > 0) {
                        int cp = buf.codePointBefore(cursor);
                        int charLen = Character.charCount(cp);
                        buf.delete(cursor - charLen, cursor);
                        cursor -= charLen;
                        redraw(buf, cursor);
                    }
                    continue;
                }
                if (ch == 27) {
                    int a = in.read();
                    if (a == -1) { insertCodePoint(buf, cursor, ch); cursor++; redraw(buf, cursor); continue; }
                    if (a == '[') {
                        int b = in.read();
                        if (b == 'A') {
                            if (histIdx > 0) {
                                if (histIdx == history.size()) saved = buf.toString();
                                histIdx--;
                                buf.setLength(0);
                                buf.append(history.get(histIdx));
                                cursor = buf.length();
                                redraw(buf, cursor);
                            }
                            continue;
                        }
                        if (b == 'B') {
                            if (histIdx < history.size()) {
                                histIdx++;
                                buf.setLength(0);
                                if (histIdx == history.size()) {
                                    buf.append(saved);
                                } else {
                                    buf.append(history.get(histIdx));
                                }
                                cursor = buf.length();
                                redraw(buf, cursor);
                            }
                            continue;
                        }
                        if (b == 'C') {
                            if (cursor < buf.length()) {
                                cursor += Character.charCount(buf.codePointAt(cursor));
                                redraw(buf, cursor);
                            }
                            continue;
                        }
                        if (b == 'D') {
                            if (cursor > 0) {
                                cursor -= Character.charCount(buf.codePointBefore(cursor));
                                redraw(buf, cursor);
                            }
                            continue;
                        }
                        if (b == 'H') { cursor = 0; redraw(buf, cursor); continue; }
                        if (b == 'F') { cursor = buf.length(); redraw(buf, cursor); continue; }
                        if (b >= '1' && b <= '9') {
                            int d = in.read();
                            if (d == '~') {
                                if (b == '1') { cursor = 0; redraw(buf, cursor); }
                                else if (b == '4') { cursor = buf.length(); redraw(buf, cursor); }
                                else if (b == '3' && cursor < buf.length()) { buf.deleteCharAt(cursor); redraw(buf, cursor); }
                            }
                            continue;
                        }
                        if (b == '3') {
                            int d = in.read();
                            if (d == '~' && cursor < buf.length()) {
                                buf.deleteCharAt(cursor);
                                redraw(buf, cursor);
                            }
                            continue;
                        }
                    }
                    continue;
                }
                if (ch >= 32 && !Character.isISOControl(ch)) {
                    insertCodePoint(buf, cursor, ch);
                    cursor += Character.charCount(ch);
                    redraw(buf, cursor);
                    continue;
                }
            }
        } catch (IOException e) {
            return false;
        }
    }

    private static void insertCodePoint(StringBuilder buf, int pos, int cp) {
        if (pos >= buf.length()) {
            buf.appendCodePoint(cp);
        } else {
            buf.insert(pos, new String(Character.toChars(cp)));
        }
    }

    private static int displayWidth(StringBuilder buf, int start, int end) {
        int w = 0;
        int i = start;
        while (i < end) {
            int cp = buf.codePointAt(i);
            i += Character.charCount(cp);
            w += Character.toString(cp).length();
        }
        return w;
    }

    private static void redraw(StringBuilder buf, int cursor) {
        System.out.print("\r" + PROMPT + buf.toString() + "\033[K");
        int col = PROMPT_LEN + displayWidth(buf, 0, cursor);
        System.out.print("\033[" + (col + 1) + "G");
        System.out.flush();
    }

    private static String stty(String args) {
        try {
            Process p = new ProcessBuilder("sh", "-c", "stty " + args + " < /dev/tty")
                    .redirectErrorStream(true).start();
            byte[] buf = new byte[1024];
            int n = p.getInputStream().read(buf);
            p.waitFor();
            return n > 0 ? new String(buf, 0, n).trim() : "";
        } catch (Exception e) {
            return "";
        }
    }

    private static void sttyRaw() {
        stty("raw -echo opost onlcr");
    }
}
