package com.jasqlite;

import com.jasqlite.store.Database;
import com.jasqlite.store.Result;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class BasicQueryTest extends BaseTest {

    @Test
    void testCreateTableAndInsert() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
        db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)");
        db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)");

        Result result = db.execute("SELECT name, age FROM users");
        assertEquals(2, result.getRowCount());
        assertEquals("Alice", result.getValue(0, 0).asString());
        assertEquals(30, result.getValue(0, 1).asLong());
        assertEquals("Bob", result.getValue(1, 0).asString());
        assertEquals(25, result.getValue(1, 1).asLong());
    }

    @Test
    void testSelectWithWhere() throws Exception {
        db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)");
        db.execute("INSERT INTO products (name, price) VALUES ('Widget', 9.99)");
        db.execute("INSERT INTO products (name, price) VALUES ('Gadget', 19.99)");
        db.execute("INSERT INTO products (name, price) VALUES ('Doohickey', 5.99)");

        Result result = db.execute("SELECT name, price FROM products WHERE price > 7.0");
        assertEquals(2, result.getRowCount());
        assertEquals("Widget", result.getValue(0, 0).asString());
        assertEquals("Gadget", result.getValue(1, 0).asString());
    }

    @Test
    void testSelectWithoutFrom() throws Exception {
        Result result = db.execute("SELECT 1 + 2");
        assertEquals(1, result.getRowCount());
        assertEquals(3.0, result.getValue(0, 0).asDouble());
    }

    @Test
    void testAggregateFunctions() throws Exception {
        db.execute("CREATE TABLE scores (name TEXT, score INTEGER)");
        db.execute("INSERT INTO scores (name, score) VALUES ('Alice', 90)");
        db.execute("INSERT INTO scores (name, score) VALUES ('Bob', 80)");
        db.execute("INSERT INTO scores (name, score) VALUES ('Charlie', 95)");

        Result countResult = db.execute("SELECT COUNT(*) FROM scores");
        assertTrue(countResult.getRowCount() > 0);
    }

    @Test
    void testOrderBy() throws Exception {
        db.execute("CREATE TABLE items (name TEXT, value INTEGER)");
        db.execute("INSERT INTO items (name, value) VALUES ('C', 3)");
        db.execute("INSERT INTO items (name, value) VALUES ('A', 1)");
        db.execute("INSERT INTO items (name, value) VALUES ('B', 2)");

        Result result = db.execute("SELECT name FROM items ORDER BY name ASC");
        assertEquals(3, result.getRowCount());
        assertEquals("A", result.getValue(0, 0).asString());
        assertEquals("B", result.getValue(1, 0).asString());
        assertEquals("C", result.getValue(2, 0).asString());
    }

    @Test
    void testLimitOffset() throws Exception {
        db.execute("CREATE TABLE nums (val INTEGER)");
        for (int i = 1; i <= 10; i++) {
            db.execute("INSERT INTO nums (val) VALUES (" + i + ")");
        }

        Result result = db.execute("SELECT val FROM nums ORDER BY val LIMIT 3 OFFSET 2");
        assertEquals(3, result.getRowCount());
        assertEquals(3.0, result.getValue(0, 0).asDouble());
        assertEquals(4.0, result.getValue(1, 0).asDouble());
        assertEquals(5.0, result.getValue(2, 0).asDouble());
    }

    @Test
    void testStringFunctions() throws Exception {
        Result result = db.execute("SELECT UPPER('hello'), LOWER('WORLD'), LENGTH('test')");
        assertEquals(1, result.getRowCount());
        assertEquals("HELLO", result.getValue(0, 0).asString());
        assertEquals("world", result.getValue(0, 1).asString());
        assertEquals(4, result.getValue(0, 2).asLong());
    }

    @Test
    void testMathFunctions() throws Exception {
        Result result = db.execute("SELECT ABS(-5), ROUND(3.14159, 2)");
        assertEquals(1, result.getRowCount());
        assertEquals(5, result.getValue(0, 0).asLong());
        assertEquals(3.14, result.getValue(0, 1).asDouble(), 0.001);
    }

    @Test
    void testDistinct() throws Exception {
        db.execute("CREATE TABLE t (val INTEGER)");
        db.execute("INSERT INTO t (val) VALUES (1)");
        db.execute("INSERT INTO t (val) VALUES (1)");
        db.execute("INSERT INTO t (val) VALUES (2)");
        db.execute("INSERT INTO t (val) VALUES (2)");
        db.execute("INSERT INTO t (val) VALUES (3)");

        Result result = db.execute("SELECT DISTINCT val FROM t ORDER BY val");
        assertEquals(3, result.getRowCount());
    }

    @Test
    void testBinaryCompatibilityHeader() throws Exception {
        String path = tempDir.resolve("test-compat.db").toString();
        new File(path).delete();
        Database jaDb = new Database(path);
        jaDb.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)");
        jaDb.execute("INSERT INTO test (val) VALUES ('hello')");
        jaDb.close();

        byte[] header = new byte[16];
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(path, "r")) {
            raf.readFully(header);
        }
        String headerStr = new String(header);
        assertTrue(headerStr.startsWith("SQLite format 3"),
            "Database file should have SQLite header, got: " + headerStr);
    }

    @Test
    void testPreparedStatement() throws Exception {
        String path = tempDir.resolve("test-ps.db").toString();
        new File(path).delete();

        try (Connection conn = DriverManager.getConnection("jdbc:jasqlite:" + path)) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");

            PreparedStatement ps = conn.prepareStatement("INSERT INTO test (name) VALUES (?)");
            ps.setString(1, "Hello");
            ps.executeUpdate();

            ResultSet rs = stmt.executeQuery("SELECT name FROM test");
            assertTrue(rs.next());
            assertEquals("Hello", rs.getString("name"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testDatabaseMetadata() throws Exception {
        String path = tempDir.resolve("test-meta.db").toString();
        new File(path).delete();

        try (Connection conn = DriverManager.getConnection("jdbc:jasqlite:" + path)) {
            DatabaseMetaData meta = conn.getMetaData();
            assertEquals("JaSQLite", meta.getDatabaseProductName());
            assertEquals(1, meta.getDriverMajorVersion());
        }
    }
}
