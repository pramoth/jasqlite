package com.jasqlite;

import com.jasqlite.store.Database;
import com.jasqlite.store.Result;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class PartialIndexTest extends BaseTest {

    @Test
    void testPartialIndexOnlyIndexesMatchingRows() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)");
        db.execute("INSERT INTO users (name, active) VALUES ('Alice', 1)");
        db.execute("INSERT INTO users (name, active) VALUES ('Bob', 0)");
        db.execute("INSERT INTO users (name, active) VALUES ('Charlie', 1)");

        db.execute("CREATE INDEX idx_active_names ON users (name) WHERE active = 1");

        Result all = db.execute("SELECT name FROM users ORDER BY id");
        assertEquals(3, all.getRowCount());
        assertEquals("Alice", all.getValue(0, 0).asString());
        assertEquals("Bob", all.getValue(1, 0).asString());
        assertEquals("Charlie", all.getValue(2, 0).asString());
    }

    @Test
    void testPartialIndexUniqueAllowsDupsOutsideWhere() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, active INTEGER)");
        db.execute("INSERT INTO users (email, active) VALUES ('a@test.com', 1)");
        db.execute("INSERT INTO users (email, active) VALUES ('a@test.com', 0)");

        db.execute("CREATE UNIQUE INDEX idx_active_email ON users (email) WHERE active = 1");

        Result all = db.execute("SELECT email FROM users ORDER BY id");
        assertEquals(2, all.getRowCount());
    }

    @Test
    void testPartialIndexUniqueRejectsDupsInsideWhere() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, active INTEGER)");
        db.execute("INSERT INTO users (email, active) VALUES ('a@test.com', 1)");

        db.execute("CREATE UNIQUE INDEX idx_active_email ON users (email) WHERE active = 1");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("INSERT INTO users (email, active) VALUES ('a@test.com', 1)"));
        assertTrue(ex.getMessage().contains("UNIQUE constraint failed"));
    }

    @Test
    void testPartialIndexUniqueAllowsInsertAfterDeactivate() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, active INTEGER)");
        db.execute("CREATE UNIQUE INDEX idx_active_email ON users (email) WHERE active = 1");

        db.execute("INSERT INTO users (email, active) VALUES ('a@test.com', 1)");
        db.execute("UPDATE users SET active = 0 WHERE email = 'a@test.com'");
        db.execute("INSERT INTO users (email, active) VALUES ('a@test.com', 1)");

        Result result = db.execute("SELECT email FROM users ORDER BY id");
        assertEquals(2, result.getRowCount());
    }

    @Test
    void testPartialIndexOnCreateFiltersExisting() throws Exception {
        db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, deleted INTEGER)");
        db.execute("INSERT INTO products (name, deleted) VALUES ('Widget', 0)");
        db.execute("INSERT INTO products (name, deleted) VALUES ('Gadget', 1)");
        db.execute("INSERT INTO products (name, deleted) VALUES ('Doohickey', 0)");

        db.execute("CREATE INDEX idx_active_products ON products (name) WHERE deleted = 0");

        Result all = db.execute("SELECT name FROM products ORDER BY id");
        assertEquals(3, all.getRowCount());
    }

    @Test
    void testPartialIndexWithComplexWhere() throws Exception {
        db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, amount REAL)");
        db.execute("INSERT INTO orders (status, amount) VALUES ('pending', 100.0)");
        db.execute("INSERT INTO orders (status, amount) VALUES ('shipped', 200.0)");
        db.execute("INSERT INTO orders (status, amount) VALUES ('pending', 50.0)");

        db.execute("CREATE INDEX idx_pending_orders ON orders (id) WHERE status = 'pending'");

        Result all = db.execute("SELECT status FROM orders ORDER BY id");
        assertEquals(3, all.getRowCount());
    }

    @Test
    void testPartialIndexNullWhere() throws Exception {
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, archived INTEGER)");
        db.execute("INSERT INTO docs (title, archived) VALUES ('Doc1', NULL)");
        db.execute("INSERT INTO docs (title, archived) VALUES ('Doc2', 1)");
        db.execute("INSERT INTO docs (title, archived) VALUES ('Doc3', NULL)");

        db.execute("CREATE INDEX idx_non_archived ON docs (title) WHERE archived IS NULL");

        Result all = db.execute("SELECT title FROM docs ORDER BY id");
        assertEquals(3, all.getRowCount());
    }

    @Test
    void testPartialIndexPersistsAfterReload() throws Exception {
        String path = tempDir.resolve("test-partial-persist.db").toString();
        new File(path).delete();

        Database db1 = new Database(path);
        db1.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)");
        db1.execute("INSERT INTO users (name, active) VALUES ('Alice', 1)");
        db1.execute("CREATE UNIQUE INDEX idx_active_name ON users (name) WHERE active = 1");
        db1.close();

        Database db2 = new Database(path);
        db2.execute("INSERT INTO users (name, active) VALUES ('Alice', 0)");
        db2.close();

        Database db3 = new Database(path);
        SQLException ex = assertThrows(SQLException.class, () ->
            db3.execute("INSERT INTO users (name, active) VALUES ('Alice', 1)"));
        assertTrue(ex.getMessage().contains("UNIQUE constraint failed"));
        db3.close();
    }

    @Test
    void testPartialIndexViaJdbc() throws Exception {
        String path = tempDir.resolve("test-partial-jdbc.db").toString();
        new File(path).delete();

        try (Connection conn = DriverManager.getConnection("jdbc:jasqlite:" + path)) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, active INTEGER)");
            stmt.executeUpdate("INSERT INTO users (email, active) VALUES ('alice@test.com', 1)");
            stmt.execute("CREATE UNIQUE INDEX idx_active_email ON users (email) WHERE active = 1");

            SQLException ex = assertThrows(SQLException.class, () ->
                stmt.executeUpdate("INSERT INTO users (email, active) VALUES ('alice@test.com', 1)"));
            assertTrue(ex.getMessage().contains("UNIQUE constraint failed"));

            stmt.executeUpdate("INSERT INTO users (email, active) VALUES ('alice@test.com', 0)");
        }
    }

    @Test
    void testPartialIndexExplainPlan() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, active INTEGER)");
        db.execute("CREATE INDEX idx_active_email ON users (email) WHERE active = 1");

        Result result = db.execute("EXPLAIN QUERY PLAN SELECT * FROM users WHERE email = 'a@test.com' AND active = 1");

        boolean foundSearch = false;
        for (int i = 0; i < result.getRowCount(); i++) {
            String detail = result.getValue(i, 3).asString();
            if (detail.contains("SEARCH TABLE users") && detail.contains("USING INDEX idx_active_email")) {
                foundSearch = true;
            }
        }
        assertTrue(foundSearch, "Expected SEARCH using partial index");
    }
}
