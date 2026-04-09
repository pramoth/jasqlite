package com.jasqlite;

import com.jasqlite.store.Database;
import com.jasqlite.store.Result;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class IndexTest extends BaseTest {

    @Test
    void testCreateIndex() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)");
        db.execute("INSERT INTO users (email) VALUES ('alice@example.com')");
        db.execute("CREATE INDEX idx_email ON users (email)");
        assertTrue(true);
    }

    @Test
    void testCreateUniqueIndex() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, username TEXT)");
        db.execute("INSERT INTO users (email, username) VALUES ('alice@test.com', 'alice')");
        db.execute("INSERT INTO users (email, username) VALUES ('bob@test.com', 'bob')");

        db.execute("CREATE UNIQUE INDEX idx_users_email ON users (email)");

        Result tables = db.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_users_email'");
        assertEquals(1, tables.getRowCount());
        assertEquals("idx_users_email", tables.getValue(0, 0).asString());
    }

    @Test
    void testCreateUniqueIndexOnMultipleColumns() throws Exception {
        db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, order_date TEXT, order_num INTEGER)");
        db.execute("INSERT INTO orders (customer_id, order_date, order_num) VALUES (1, '2024-01-01', 100)");
        db.execute("INSERT INTO orders (customer_id, order_date, order_num) VALUES (1, '2024-01-02', 101)");
        db.execute("INSERT INTO orders (customer_id, order_date, order_num) VALUES (2, '2024-01-01', 102)");

        db.execute("CREATE UNIQUE INDEX idx_orders_cust_date ON orders (customer_id, order_date)");

        Result indexes = db.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_orders_cust_date'");
        assertEquals(1, indexes.getRowCount());
    }

    @Test
    void testUniqueIndexViaColumnConstraint() throws Exception {
        db.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, email TEXT UNIQUE, username TEXT)");

        db.execute("INSERT INTO accounts (email, username) VALUES ('alice@test.com', 'alice')");
        db.execute("INSERT INTO accounts (email, username) VALUES ('bob@test.com', 'bob')");

        Result result = db.execute("SELECT username FROM accounts WHERE email = 'alice@test.com'");
        assertEquals(1, result.getRowCount());
        assertEquals("alice", result.getValue(0, 0).asString());
    }

    @Test
    void testUniqueIndexWithNulls() throws Exception {
        db.execute("CREATE TABLE contacts (id INTEGER PRIMARY KEY, phone TEXT UNIQUE, name TEXT)");

        db.execute("INSERT INTO contacts (phone, name) VALUES ('555-0001', 'Alice')");
        db.execute("INSERT INTO contacts (phone, name) VALUES (NULL, 'Bob')");
        db.execute("INSERT INTO contacts (phone, name) VALUES (NULL, 'Charlie')");

        Result result = db.execute("SELECT name FROM contacts ORDER BY name");
        assertEquals(3, result.getRowCount());
        assertEquals("Alice", result.getValue(0, 0).asString());
        assertEquals("Bob", result.getValue(1, 0).asString());
        assertEquals("Charlie", result.getValue(2, 0).asString());
    }

    @Test
    void testUniqueIndexIfNotExists() throws Exception {
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT)");
        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_items_code ON items (code)");

        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_items_code ON items (code)");

        Result indexes = db.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_items_code'");
        assertEquals(1, indexes.getRowCount());
    }

    @Test
    void testDropUniqueIndex() throws Exception {
        db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, sku TEXT)");
        db.execute("CREATE UNIQUE INDEX idx_products_sku ON products (sku)");

        Result before = db.execute("SELECT name FROM sqlite_master WHERE type='index'");
        assertTrue(before.getRowCount() >= 1);

        db.execute("DROP INDEX idx_products_sku");

        Result after = db.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_products_sku'");
        assertEquals(0, after.getRowCount());
    }

    @Test
    void testUniqueIndexAndJdbcMetadata() throws Exception {
        String path = tempDir.resolve("test-idx-meta.db").toString();
        new File(path).delete();

        try (Connection conn = DriverManager.getConnection("jdbc:jasqlite:" + path)) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, username TEXT)");
            stmt.execute("CREATE UNIQUE INDEX idx_users_username ON users (username)");

            DatabaseMetaData meta = conn.getMetaData();

            ResultSet tables = meta.getTables(null, null, "users", new String[]{"TABLE"});
            assertTrue(tables.next());
            assertEquals("users", tables.getString("TABLE_NAME"));

            ResultSet columns = meta.getColumns(null, null, "users", null);
            assertTrue(columns.next());
            assertEquals("id", columns.getString("COLUMN_NAME"));
            assertTrue(columns.next());
            assertEquals("email", columns.getString("COLUMN_NAME"));
            assertTrue(columns.next());
            assertEquals("username", columns.getString("COLUMN_NAME"));

            ResultSet indexes = meta.getIndexInfo(null, null, "users", true, false);
            assertTrue(indexes.next());
            String idxName = indexes.getString("INDEX_NAME");
            assertTrue(idxName.equals("idx_users_username") || idxName.contains("autoindex"),
                "Expected unique index, got: " + idxName);
        }
    }

    @Test
    void testUniqueIndexRejectsDuplicateOnInsert() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)");
        db.execute("CREATE UNIQUE INDEX idx_email ON users (email)");
        db.execute("INSERT INTO users (email) VALUES ('alice@test.com')");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("INSERT INTO users (email) VALUES ('alice@test.com')"));
        assertTrue(ex.getMessage().contains("UNIQUE constraint failed"),
            "Expected UNIQUE constraint failed, got: " + ex.getMessage());
    }

    @Test
    void testUniqueIndexAllowsDifferentValues() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)");
        db.execute("CREATE UNIQUE INDEX idx_email ON users (email)");
        db.execute("INSERT INTO users (email) VALUES ('alice@test.com')");
        db.execute("INSERT INTO users (email) VALUES ('bob@test.com')");

        Result result = db.execute("SELECT email FROM users ORDER BY email");
        assertEquals(2, result.getRowCount());
        assertEquals("alice@test.com", result.getValue(0, 0).asString());
        assertEquals("bob@test.com", result.getValue(1, 0).asString());
    }

    @Test
    void testUniqueIndexRejectsDuplicateOnCreate() throws Exception {
        db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, sku TEXT)");
        db.execute("INSERT INTO products (sku) VALUES ('ABC')");
        db.execute("INSERT INTO products (sku) VALUES ('ABC')");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("CREATE UNIQUE INDEX idx_sku ON products (sku)"));
        assertTrue(ex.getMessage().contains("UNIQUE constraint failed"),
            "Expected UNIQUE constraint failed, got: " + ex.getMessage());
    }

    @Test
    void testUniqueCompositeIndexRejectsDuplicate() throws Exception {
        db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, order_num INTEGER)");
        db.execute("CREATE UNIQUE INDEX idx_cust_order ON orders (customer_id, order_num)");
        db.execute("INSERT INTO orders (customer_id, order_num) VALUES (1, 100)");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("INSERT INTO orders (customer_id, order_num) VALUES (1, 100)"));
        assertTrue(ex.getMessage().contains("UNIQUE constraint failed"),
            "Expected UNIQUE constraint failed, got: " + ex.getMessage());
    }

    @Test
    void testUniqueCompositeIndexAllowsPartialOverlap() throws Exception {
        db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, order_num INTEGER)");
        db.execute("CREATE UNIQUE INDEX idx_cust_order ON orders (customer_id, order_num)");
        db.execute("INSERT INTO orders (customer_id, order_num) VALUES (1, 100)");
        db.execute("INSERT INTO orders (customer_id, order_num) VALUES (1, 101)");
        db.execute("INSERT INTO orders (customer_id, order_num) VALUES (2, 100)");

        Result result = db.execute("SELECT customer_id, order_num FROM orders ORDER BY id");
        assertEquals(3, result.getRowCount());
        assertEquals(1, result.getValue(0, 0).asLong());
        assertEquals(100, result.getValue(0, 1).asLong());
        assertEquals(1, result.getValue(1, 0).asLong());
        assertEquals(101, result.getValue(1, 1).asLong());
        assertEquals(2, result.getValue(2, 0).asLong());
        assertEquals(100, result.getValue(2, 1).asLong());
    }

    @Test
    void testUniqueIndexAllowsNullDuplicates() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)");
        db.execute("CREATE UNIQUE INDEX idx_email ON users (email)");
        db.execute("INSERT INTO users (email) VALUES (NULL)");
        db.execute("INSERT INTO users (email) VALUES (NULL)");

        Result result = db.execute("SELECT id FROM users ORDER BY id");
        assertEquals(2, result.getRowCount());
    }

    @Test
    void testUniqueConstraintViaJdbc() throws Exception {
        String path = tempDir.resolve("test-unique-jdbc.db").toString();
        new File(path).delete();

        try (Connection conn = DriverManager.getConnection("jdbc:jasqlite:" + path)) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, username TEXT)");
            stmt.execute("CREATE UNIQUE INDEX idx_username ON accounts (username)");
            stmt.executeUpdate("INSERT INTO accounts (username) VALUES ('alice')");

            SQLException ex = assertThrows(SQLException.class, () ->
                stmt.executeUpdate("INSERT INTO accounts (username) VALUES ('alice')"));
            assertTrue(ex.getMessage().contains("UNIQUE constraint failed"));
        }
    }
}
