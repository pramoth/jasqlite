package com.jasqlite;

import com.jasqlite.store.Database;
import com.jasqlite.store.Result;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class AlterTableTest extends BaseTest {

    @Test
    void testAlterTableRenameTo() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("INSERT INTO users (name) VALUES ('Alice')");

        db.execute("ALTER TABLE users RENAME TO people");

        Result result = db.execute("SELECT name FROM people");
        assertEquals(1, result.getRowCount());
        assertEquals("Alice", result.getValue(0, 0).asString());

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("SELECT * FROM users"));
        assertTrue(ex.getMessage().contains("no such table"));
    }

    @Test
    void testAlterTableRenameToDuplicateName() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("ALTER TABLE users RENAME TO people"));
        assertTrue(ex.getMessage().contains("already another table"));
    }

    @Test
    void testAlterTableRenameColumn() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("INSERT INTO users (name) VALUES ('Alice')");

        db.execute("ALTER TABLE users RENAME COLUMN name TO username");

        Result result = db.execute("SELECT username FROM users");
        assertEquals(1, result.getRowCount());
        assertEquals("Alice", result.getValue(0, 0).asString());
    }

    @Test
    void testAlterTableRenameColumnNoSuchColumn() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("ALTER TABLE users RENAME COLUMN missing TO other"));
        assertTrue(ex.getMessage().contains("no such column"));
    }

    @Test
    void testAlterTableRenameColumnDuplicate() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("ALTER TABLE users RENAME COLUMN name TO email"));
        assertTrue(ex.getMessage().contains("duplicate column name"));
    }

    @Test
    void testAlterTableAddColumn() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("INSERT INTO users (name) VALUES ('Alice')");

        db.execute("ALTER TABLE users ADD COLUMN email TEXT");

        Result result = db.execute("SELECT name, email FROM users");
        assertEquals(1, result.getRowCount());
        assertEquals("Alice", result.getValue(0, 0).asString());
    }

    @Test
    void testAlterTableAddColumnWithDefault() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("INSERT INTO users (name) VALUES ('Alice')");

        db.execute("ALTER TABLE users ADD COLUMN active INTEGER DEFAULT 1");

        Result result = db.execute("SELECT name FROM users");
        assertEquals(1, result.getRowCount());
        assertEquals("Alice", result.getValue(0, 0).asString());
    }

    @Test
    void testAlterTableAddColumnDuplicate() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("ALTER TABLE users ADD COLUMN name TEXT"));
        assertTrue(ex.getMessage().contains("duplicate column name"));
    }

    @Test
    void testAlterTableDropColumn() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");
        db.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com')");

        db.execute("ALTER TABLE users DROP COLUMN email");

        Result result = db.execute("SELECT name FROM users");
        assertEquals(1, result.getRowCount());
        assertEquals("Alice", result.getValue(0, 0).asString());
    }

    @Test
    void testAlterTableDropColumnNoSuchColumn() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("ALTER TABLE users DROP COLUMN missing"));
        assertTrue(ex.getMessage().contains("no such column"));
    }

    @Test
    void testAlterTableDropColumnPrimaryKey() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("ALTER TABLE users DROP COLUMN id"));
        assertTrue(ex.getMessage().contains("PRIMARY KEY"));
    }

    @Test
    void testAlterTableDropLastColumn() throws Exception {
        db.execute("CREATE TABLE users (name TEXT)");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("ALTER TABLE users DROP COLUMN name"));
        assertTrue(ex.getMessage().contains("at least one column"));
    }

    @Test
    void testAlterTableRenamePersistsAfterReload() throws Exception {
        String path = tempDir.resolve("test-alter-persist.db").toString();
        new File(path).delete();

        Database db1 = new Database(path);
        db1.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        db1.execute("INSERT INTO users (name) VALUES ('Alice')");
        db1.execute("ALTER TABLE users RENAME TO people");
        db1.close();

        Database db2 = new Database(path);
        Result result = db2.execute("SELECT name FROM people");
        assertEquals(1, result.getRowCount());
        assertEquals("Alice", result.getValue(0, 0).asString());
        db2.close();
    }

    @Test
    void testAlterTableAddColumnPersistsAfterReload() throws Exception {
        String path = tempDir.resolve("test-alter-add-persist.db").toString();
        new File(path).delete();

        Database db1 = new Database(path);
        db1.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        db1.execute("INSERT INTO users (name) VALUES ('Alice')");
        db1.execute("ALTER TABLE users ADD COLUMN email TEXT");
        db1.close();

        Database db2 = new Database(path);
        Result result = db2.execute("SELECT name FROM users");
        assertEquals(1, result.getRowCount());
        assertEquals("Alice", result.getValue(0, 0).asString());
        db2.close();
    }

    @Test
    void testAlterTableViaJdbc() throws Exception {
        String path = tempDir.resolve("test-alter-jdbc.db").toString();
        new File(path).delete();

        try (Connection conn = DriverManager.getConnection("jdbc:jasqlite:" + path)) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.executeUpdate("INSERT INTO users (name) VALUES ('Alice')");

            stmt.execute("ALTER TABLE users ADD COLUMN email TEXT");
            stmt.execute("ALTER TABLE users RENAME COLUMN name TO username");
            stmt.execute("ALTER TABLE users RENAME TO people");

            ResultSet rs = stmt.executeQuery("SELECT username FROM people");
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString("username"));
        }
    }
}
