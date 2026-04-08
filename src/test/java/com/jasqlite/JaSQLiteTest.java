package com.jasqlite;

import com.jasqlite.store.Database;
import com.jasqlite.store.Result;
import com.jasqlite.util.SQLiteValue;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;

class JaSQLiteTest {

    private static Path tempDir;
    private Database db;

    @BeforeAll
    static void setup() throws Exception {
        tempDir = Files.createTempDirectory("jasqlite-test");
        Class.forName("com.jasqlite.jdbc.JaSQLiteDriver");
    }

    @AfterAll
    static void cleanup() throws Exception {
        // Best effort cleanup
    }

    @BeforeEach
    void openDb() throws Exception {
        String path = tempDir.resolve("test.db").toString();
        new File(path).delete();
        db = new Database(path);
    }

    @AfterEach
    void closeDb() throws Exception {
        if (db != null) db.close();
    }

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
    void testCreateIndex() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)");
        db.execute("INSERT INTO users (email) VALUES ('alice@example.com')");
        db.execute("CREATE INDEX idx_email ON users (email)");
        // If we got here without error, index was created
        assertTrue(true);
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

    @Test
    void testBinaryCompatibilityHeader() throws Exception {
        // Create a database with JaSQLite
        String path = tempDir.resolve("test-compat.db").toString();
        new File(path).delete();
        Database jaDb = new Database(path);
        jaDb.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)");
        jaDb.execute("INSERT INTO test (val) VALUES ('hello')");
        jaDb.close();

        // Verify the SQLite header is present
        byte[] header = new byte[16];
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(path, "r")) {
            raf.readFully(header);
        }
        String headerStr = new String(header);
        assertTrue(headerStr.startsWith("SQLite format 3"), 
            "Database file should have SQLite header, got: " + headerStr);
    }

    // ==================== JOIN Tests ====================

    @Test
    void testInnerJoin() throws Exception {
        db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)");

        db.execute("INSERT INTO departments (name) VALUES ('Engineering')");
        db.execute("INSERT INTO departments (name) VALUES ('Marketing')");
        db.execute("INSERT INTO departments (name) VALUES ('Sales')");

        db.execute("INSERT INTO employees (name, dept_id) VALUES ('Alice', 1)");
        db.execute("INSERT INTO employees (name, dept_id) VALUES ('Bob', 2)");
        db.execute("INSERT INTO employees (name, dept_id) VALUES ('Charlie', 1)");
        db.execute("INSERT INTO employees (name, dept_id) VALUES ('Diana', 3)");

        Result result = db.execute(
            "SELECT e.name, d.name FROM employees e INNER JOIN departments d ON e.dept_id = d.id ORDER BY e.name"
        );
        assertEquals(4, result.getRowCount());

        // Alice -> Engineering
        assertEquals("Alice", result.getValue(0, 0).asString());
        assertEquals("Engineering", result.getValue(0, 1).asString());

        // Bob -> Marketing
        assertEquals("Bob", result.getValue(1, 0).asString());
        assertEquals("Marketing", result.getValue(1, 1).asString());

        // Charlie -> Engineering
        assertEquals("Charlie", result.getValue(2, 0).asString());
        assertEquals("Engineering", result.getValue(2, 1).asString());

        // Diana -> Sales
        assertEquals("Diana", result.getValue(3, 0).asString());
        assertEquals("Sales", result.getValue(3, 1).asString());
    }

    @Test
    void testLeftJoin() throws Exception {
        db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, product TEXT, customer_id INTEGER)");

        db.execute("INSERT INTO customers (name) VALUES ('Alice')");
        db.execute("INSERT INTO customers (name) VALUES ('Bob')");
        db.execute("INSERT INTO customers (name) VALUES ('Charlie')");

        // Alice has 2 orders, Bob has 1, Charlie has none
        db.execute("INSERT INTO orders (product, customer_id) VALUES ('Laptop', 1)");
        db.execute("INSERT INTO orders (product, customer_id) VALUES ('Phone', 1)");
        db.execute("INSERT INTO orders (product, customer_id) VALUES ('Tablet', 2)");

        Result result = db.execute(
            "SELECT c.name, o.product FROM customers c LEFT JOIN orders o ON c.id = o.customer_id ORDER BY c.name"
        );

        // Should return 4 rows: Alice(Laptop), Alice(Phone), Bob(Tablet), Charlie(NULL)
        assertEquals(4, result.getRowCount());

        assertEquals("Alice", result.getValue(0, 0).asString());
        // One of Alice's orders
        assertTrue("Laptop".equals(result.getValue(0, 1).asString()) ||
                   "Phone".equals(result.getValue(0, 1).asString()));

        assertEquals("Alice", result.getValue(1, 0).asString());
        assertTrue("Laptop".equals(result.getValue(1, 1).asString()) ||
                   "Phone".equals(result.getValue(1, 1).asString()));

        assertEquals("Bob", result.getValue(2, 0).asString());
        assertEquals("Tablet", result.getValue(2, 1).asString());

        // Charlie has no orders - product should be NULL
        assertEquals("Charlie", result.getValue(3, 0).asString());
        assertNull(result.getValue(3, 1).asString());
    }

    @Test
    void testJoinWithWhereClause() throws Exception {
        db.execute("CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER, price REAL)");

        db.execute("INSERT INTO authors (name) VALUES ('Tolkien')");
        db.execute("INSERT INTO authors (name) VALUES ('Asimov')");
        db.execute("INSERT INTO authors (name) VALUES ('Clarke')");

        db.execute("INSERT INTO books (title, author_id, price) VALUES ('The Hobbit', 1, 12.99)");
        db.execute("INSERT INTO books (title, author_id, price) VALUES ('Foundation', 2, 9.99)");
        db.execute("INSERT INTO books (title, author_id, price) VALUES ('2001', 3, 14.99)");
        db.execute("INSERT INTO books (title, author_id, price) VALUES ('LOTR', 1, 19.99)");
        db.execute("INSERT INTO books (title, author_id, price) VALUES ('I Robot', 2, 8.99)");

        // Find expensive books with their authors
        Result result = db.execute(
            "SELECT b.title, a.name, b.price FROM books b " +
            "INNER JOIN authors a ON b.author_id = a.id " +
            "WHERE b.price > 10.0 ORDER BY b.price DESC"
        );

        assertEquals(3, result.getRowCount());
        assertEquals("LOTR", result.getValue(0, 0).asString());
        assertEquals("Tolkien", result.getValue(0, 1).asString());
        assertEquals(19.99, result.getValue(0, 2).asDouble(), 0.01);

        assertEquals("2001", result.getValue(1, 0).asString());
        assertEquals("Clarke", result.getValue(1, 1).asString());

        assertEquals("The Hobbit", result.getValue(2, 0).asString());
        assertEquals("Tolkien", result.getValue(2, 1).asString());
    }

    @Test
    void testJoinWithAggregate() throws Exception {
        db.execute("CREATE TABLE projects (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT, project_id INTEGER, hours INTEGER)");

        db.execute("INSERT INTO projects (name) VALUES ('Alpha')");
        db.execute("INSERT INTO projects (name) VALUES ('Beta')");
        db.execute("INSERT INTO projects (name) VALUES ('Gamma')");

        db.execute("INSERT INTO tasks (title, project_id, hours) VALUES ('Design', 1, 8)");
        db.execute("INSERT INTO tasks (title, project_id, hours) VALUES ('Develop', 1, 20)");
        db.execute("INSERT INTO tasks (title, project_id, hours) VALUES ('Test', 1, 5)");
        db.execute("INSERT INTO tasks (title, project_id, hours) VALUES ('Plan', 2, 10)");
        db.execute("INSERT INTO tasks (title, project_id, hours) VALUES ('Build', 2, 15)");
        // Gamma has no tasks

        Result result = db.execute(
            "SELECT p.name, COUNT(*) AS task_count FROM projects p " +
            "INNER JOIN tasks t ON p.id = t.project_id " +
            "GROUP BY p.name ORDER BY p.name"
        );

        assertEquals(2, result.getRowCount());
        assertEquals("Alpha", result.getValue(0, 0).asString());
        assertEquals(3, result.getValue(0, 1).asLong());

        assertEquals("Beta", result.getValue(1, 0).asString());
        assertEquals(2, result.getValue(1, 1).asLong());
    }

    @Test
    void testMultiTableJoin() throws Exception {
        db.execute("CREATE TABLE countries (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("CREATE TABLE cities (id INTEGER PRIMARY KEY, name TEXT, country_id INTEGER)");
        db.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, city_id INTEGER)");

        db.execute("INSERT INTO countries (name) VALUES ('USA')");
        db.execute("INSERT INTO countries (name) VALUES ('UK')");

        db.execute("INSERT INTO cities (name, country_id) VALUES ('New York', 1)");
        db.execute("INSERT INTO cities (name, country_id) VALUES ('London', 2)");
        db.execute("INSERT INTO cities (name, country_id) VALUES ('Boston', 1)");

        db.execute("INSERT INTO people (name, city_id) VALUES ('Alice', 1)");
        db.execute("INSERT INTO people (name, city_id) VALUES ('Bob', 2)");
        db.execute("INSERT INTO people (name, city_id) VALUES ('Charlie', 3)");

        Result result = db.execute(
            "SELECT p.name, c.name AS city, co.name AS country " +
            "FROM people p " +
            "INNER JOIN cities c ON p.city_id = c.id " +
            "INNER JOIN countries co ON c.country_id = co.id " +
            "ORDER BY p.name"
        );

        assertEquals(3, result.getRowCount());
        assertEquals("Alice", result.getValue(0, 0).asString());
        assertEquals("New York", result.getValue(0, 1).asString());
        assertEquals("USA", result.getValue(0, 2).asString());

        assertEquals("Bob", result.getValue(1, 0).asString());
        assertEquals("London", result.getValue(1, 1).asString());
        assertEquals("UK", result.getValue(1, 2).asString());

        assertEquals("Charlie", result.getValue(2, 0).asString());
        assertEquals("Boston", result.getValue(2, 1).asString());
        assertEquals("USA", result.getValue(2, 2).asString());
    }

    // ==================== Unique Index Tests ====================

    @Test
    void testCreateUniqueIndex() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, username TEXT)");
        db.execute("INSERT INTO users (email, username) VALUES ('alice@test.com', 'alice')");
        db.execute("INSERT INTO users (email, username) VALUES ('bob@test.com', 'bob')");

        // Create unique index on email
        db.execute("CREATE UNIQUE INDEX idx_users_email ON users (email)");

        // Verify the index shows up in schema
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

        // Create unique composite index on customer_id + order_date
        db.execute("CREATE UNIQUE INDEX idx_orders_cust_date ON orders (customer_id, order_date)");

        // Verify it exists
        Result indexes = db.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_orders_cust_date'");
        assertEquals(1, indexes.getRowCount());
    }

    @Test
    void testUniqueIndexViaColumnConstraint() throws Exception {
        db.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, email TEXT UNIQUE, username TEXT)");

        db.execute("INSERT INTO accounts (email, username) VALUES ('alice@test.com', 'alice')");
        db.execute("INSERT INTO accounts (email, username) VALUES ('bob@test.com', 'bob')");

        // Verify we can query by the unique email
        Result result = db.execute("SELECT username FROM accounts WHERE email = 'alice@test.com'");
        assertEquals(1, result.getRowCount());
        assertEquals("alice", result.getValue(0, 0).asString());
    }

    @Test
    void testUniqueIndexWithNulls() throws Exception {
        db.execute("CREATE TABLE contacts (id INTEGER PRIMARY KEY, phone TEXT UNIQUE, name TEXT)");

        db.execute("INSERT INTO contacts (phone, name) VALUES ('555-0001', 'Alice')");
        // Insert a row with NULL phone (should be allowed with unique index in SQLite)
        db.execute("INSERT INTO contacts (phone, name) VALUES (NULL, 'Bob')");
        // Another NULL phone should also be allowed in SQLite
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

        // Second CREATE should not fail due to IF NOT EXISTS
        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_items_code ON items (code)");

        Result indexes = db.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_items_code'");
        assertEquals(1, indexes.getRowCount());
    }

    @Test
    void testDropUniqueIndex() throws Exception {
        db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, sku TEXT)");
        db.execute("CREATE UNIQUE INDEX idx_products_sku ON products (sku)");

        // Verify index exists
        Result before = db.execute("SELECT name FROM sqlite_master WHERE type='index'");
        assertTrue(before.getRowCount() >= 1);

        // Drop the index
        db.execute("DROP INDEX idx_products_sku");

        // Verify index is gone
        Result after = db.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_products_sku'");
        assertEquals(0, after.getRowCount());
    }

    @Test
    void testJoinViaJdbcPreparedStatement() throws Exception {
        String path = tempDir.resolve("test-join-ps.db").toString();
        new File(path).delete();

        try (Connection conn = DriverManager.getConnection("jdbc:jasqlite:" + path)) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, category_id INTEGER, price REAL)");

            stmt.executeUpdate("INSERT INTO categories (name) VALUES ('Books')");
            stmt.executeUpdate("INSERT INTO categories (name) VALUES ('Electronics')");

            PreparedStatement ps = conn.prepareStatement("INSERT INTO items (name, category_id, price) VALUES (?, ?, ?)");
            ps.setString(1, "Java Programming");
            ps.setInt(2, 1);
            ps.setDouble(3, 49.99);
            ps.executeUpdate();

            ps.setString(1, "Python Cookbook");
            ps.setInt(2, 1);
            ps.setDouble(3, 39.99);
            ps.executeUpdate();

            ps.setString(1, "USB Cable");
            ps.setInt(2, 2);
            ps.setDouble(3, 9.99);
            ps.executeUpdate();

            // Test join via JDBC
            ResultSet rs = stmt.executeQuery(
                "SELECT i.name, c.name AS category, i.price " +
                "FROM items i INNER JOIN categories c ON i.category_id = c.id " +
                "ORDER BY i.price DESC"
            );

            assertTrue(rs.next());
            assertEquals("Java Programming", rs.getString("name"));
            assertEquals("Books", rs.getString("category"));
            assertEquals(49.99, rs.getDouble("price"), 0.01);

            assertTrue(rs.next());
            assertEquals("Python Cookbook", rs.getString("name"));
            assertEquals("Books", rs.getString("category"));

            assertTrue(rs.next());
            assertEquals("USB Cable", rs.getString("name"));
            assertEquals("Electronics", rs.getString("category"));

            assertFalse(rs.next());
        }
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

            // Check tables
            ResultSet tables = meta.getTables(null, null, "users", new String[]{"TABLE"});
            assertTrue(tables.next());
            assertEquals("users", tables.getString("TABLE_NAME"));

            // Check columns
            ResultSet columns = meta.getColumns(null, null, "users", null);
            assertTrue(columns.next());
            assertEquals("id", columns.getString("COLUMN_NAME"));
            assertTrue(columns.next());
            assertEquals("email", columns.getString("COLUMN_NAME"));
            assertTrue(columns.next());
            assertEquals("username", columns.getString("COLUMN_NAME"));

            // Check indexes
            ResultSet indexes = meta.getIndexInfo(null, null, "users", true, false);
            assertTrue(indexes.next());
            String idxName = indexes.getString("INDEX_NAME");
            assertTrue(idxName.equals("idx_users_username") || idxName.contains("autoindex"),
                "Expected unique index, got: " + idxName);
        }
    }
}
