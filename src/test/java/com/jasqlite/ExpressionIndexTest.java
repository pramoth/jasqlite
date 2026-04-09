package com.jasqlite;

import com.jasqlite.store.Database;
import com.jasqlite.store.Result;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class ExpressionIndexTest extends BaseTest {

    @Test
    void testCreateIndexOnLowerExpression() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("INSERT INTO users (name) VALUES ('Alice')");
        db.execute("INSERT INTO users (name) VALUES ('BOB')");
        db.execute("INSERT INTO users (name) VALUES ('Charlie')");

        db.execute("CREATE INDEX idx_lower_name ON users (LOWER(name))");

        Result all = db.execute("SELECT name FROM users ORDER BY id");
        assertEquals(3, all.getRowCount());
        assertEquals("Alice", all.getValue(0, 0).asString());
        assertEquals("BOB", all.getValue(1, 0).asString());
        assertEquals("Charlie", all.getValue(2, 0).asString());
    }

    @Test
    void testUniqueExpressionIndexRejectsDups() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)");
        db.execute("INSERT INTO users (email) VALUES ('Alice@Example.COM')");

        db.execute("CREATE UNIQUE INDEX idx_lower_email ON users (LOWER(email))");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("INSERT INTO users (email) VALUES ('alice@example.com')"));
        assertTrue(ex.getMessage().contains("UNIQUE constraint failed"));
    }

    @Test
    void testUniqueExpressionIndexAllowsDifferentValues() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)");
        db.execute("INSERT INTO users (email) VALUES ('Alice@Example.COM')");

        db.execute("CREATE UNIQUE INDEX idx_lower_email ON users (LOWER(email))");

        db.execute("INSERT INTO users (email) VALUES ('bob@example.com')");

        Result all = db.execute("SELECT email FROM users ORDER BY id");
        assertEquals(2, all.getRowCount());
    }

    @Test
    void testUniqueExpressionIndexOnCreateRejectsExistingDups() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)");
        db.execute("INSERT INTO users (email) VALUES ('alice@test.com')");
        db.execute("INSERT INTO users (email) VALUES ('ALICE@TEST.COM')");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("CREATE UNIQUE INDEX idx_lower_email ON users (LOWER(email))"));
        assertTrue(ex.getMessage().contains("UNIQUE constraint failed"));
    }

    @Test
    void testExpressionIndexWithUpper() throws Exception {
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT)");
        db.execute("INSERT INTO items (code) VALUES ('abc')");
        db.execute("INSERT INTO items (code) VALUES ('DEF')");
        db.execute("INSERT INTO items (code) VALUES ('Ghi')");

        db.execute("CREATE INDEX idx_upper_code ON items (UPPER(code))");

        Result all = db.execute("SELECT code FROM items ORDER BY id");
        assertEquals(3, all.getRowCount());
    }

    @Test
    void testExpressionIndexPersistsAfterReload() throws Exception {
        String path = tempDir.resolve("test-expr-persist.db").toString();
        new File(path).delete();

        Database db1 = new Database(path);
        db1.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)");
        db1.execute("INSERT INTO users (email) VALUES ('Alice@Example.COM')");
        db1.execute("CREATE UNIQUE INDEX idx_lower_email ON users (LOWER(email))");
        db1.close();

        Database db2 = new Database(path);
        db2.execute("INSERT INTO users (email) VALUES ('bob@test.com')");
        SQLException ex = assertThrows(SQLException.class, () ->
            db2.execute("INSERT INTO users (email) VALUES ('alice@example.com')"));
        assertTrue(ex.getMessage().contains("UNIQUE constraint failed"));
        db2.close();
    }

    @Test
    void testExpressionIndexPersistsAfterReloadNoDuplicates() throws Exception {
        String path = tempDir.resolve("test-expr-persist2.db").toString();
        new File(path).delete();

        Database db1 = new Database(path);
        db1.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        db1.execute("INSERT INTO users (name) VALUES ('Alice')");
        db1.execute("CREATE INDEX idx_lower_name ON users (LOWER(name))");
        db1.close();

        Database db2 = new Database(path);
        db2.execute("INSERT INTO users (name) VALUES ('Bob')");
        Result all = db2.execute("SELECT name FROM users ORDER BY id");
        assertEquals(2, all.getRowCount());
        db2.close();
    }

    @Test
    void testMixedColumnAndExpressionIndex() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)");
        db.execute("INSERT INTO users (name, active) VALUES ('Alice', 1)");
        db.execute("INSERT INTO users (name, active) VALUES ('bob', 1)");

        db.execute("CREATE INDEX idx_name ON users (name)");
        db.execute("CREATE INDEX idx_lower_name ON users (LOWER(name))");

        Result all = db.execute("SELECT name FROM users ORDER BY id");
        assertEquals(2, all.getRowCount());
    }

    @Test
    void testExpressionIndexViaJdbc() throws Exception {
        String path = tempDir.resolve("test-expr-jdbc.db").toString();
        new File(path).delete();

        try (Connection conn = DriverManager.getConnection("jdbc:jasqlite:" + path)) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)");
            stmt.executeUpdate("INSERT INTO users (email) VALUES ('Alice@Example.COM')");
            stmt.execute("CREATE UNIQUE INDEX idx_lower_email ON users (LOWER(email))");

            SQLException ex = assertThrows(SQLException.class, () ->
                stmt.executeUpdate("INSERT INTO users (email) VALUES ('alice@example.com')"));
            assertTrue(ex.getMessage().contains("UNIQUE constraint failed"));

            stmt.executeUpdate("INSERT INTO users (email) VALUES ('bob@test.com')");
        }
    }

    @Test
    void testExpressionIndexWithWhereClause() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, active INTEGER)");
        db.execute("INSERT INTO users (email, active) VALUES ('Alice@test.com', 1)");
        db.execute("INSERT INTO users (email, active) VALUES ('alice@test.com', 0)");

        db.execute("CREATE UNIQUE INDEX idx_active_lower_email ON users (LOWER(email)) WHERE active = 1");

        db.execute("INSERT INTO users (email, active) VALUES ('alice@test.com', 0)");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("INSERT INTO users (email, active) VALUES ('alice@TEST.com', 1)"));
        assertTrue(ex.getMessage().contains("UNIQUE constraint failed"));
    }

    @Test
    void testExpressionIndexDeleteMaintenance() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)");
        db.execute("INSERT INTO users (email) VALUES ('alice@test.com')");

        db.execute("CREATE UNIQUE INDEX idx_lower_email ON users (LOWER(email))");

        db.execute("INSERT INTO users (email) VALUES ('bob@test.com')");
        db.execute("DELETE FROM users");

        db.execute("INSERT INTO users (email) VALUES ('ALICE@TEST.COM')");
        db.execute("INSERT INTO users (email) VALUES ('bob@test.com')");

        Result all = db.execute("SELECT email FROM users ORDER BY id");
        assertEquals(2, all.getRowCount());
    }

    @Test
    void testExpressionIndexNullValue() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)");
        db.execute("INSERT INTO users (email) VALUES (NULL)");
        db.execute("INSERT INTO users (email) VALUES (NULL)");

        db.execute("CREATE UNIQUE INDEX idx_lower_email ON users (LOWER(email))");

        Result all = db.execute("SELECT email FROM users ORDER BY id");
        assertEquals(2, all.getRowCount());
    }
}
