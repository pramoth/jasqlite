package com.jasqlite;

import com.jasqlite.store.Database;
import com.jasqlite.store.Result;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class ExplainPlanTest extends BaseTest {

    @Test
    void testExplainQueryPlanSimpleSelect() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
        db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)");
        db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)");

        Result result = db.execute("EXPLAIN QUERY PLAN SELECT * FROM users");

        assertArrayEquals(new String[]{"selectid", "order", "from", "detail"}, result.getColumnNames());
        assertTrue(result.getRowCount() >= 1);

        String detail = result.getValue(0, 3).asString();
        assertTrue(detail.contains("SCAN TABLE users"), "Expected SCAN TABLE users, got: " + detail);
    }

    @Test
    void testExplainQueryPlanSelectWithWhere() throws Exception {
        db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)");
        db.execute("INSERT INTO products (name, price) VALUES ('Widget', 9.99)");

        Result result = db.execute("EXPLAIN QUERY PLAN SELECT name FROM products WHERE price > 5.0");

        boolean foundScan = false;
        boolean foundFilter = false;
        for (int i = 0; i < result.getRowCount(); i++) {
            String detail = result.getValue(i, 3).asString();
            if (detail.contains("SCAN TABLE products")) foundScan = true;
            if (detail.contains("FILTER")) foundFilter = true;
        }
        assertTrue(foundScan, "Expected SCAN TABLE products in plan");
        assertTrue(foundFilter, "Expected FILTER in plan for WHERE clause");
    }

    @Test
    void testExplainQueryPlanSelectWithoutFrom() throws Exception {
        Result result = db.execute("EXPLAIN QUERY PLAN SELECT 1 + 2");

        assertEquals(1, result.getRowCount());
        String detail = result.getValue(0, 3).asString();
        assertEquals("SCAN CONSTANT ROW", detail);
    }

    @Test
    void testExplainQueryPlanWithJoin() throws Exception {
        db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)");

        Result result = db.execute(
            "EXPLAIN QUERY PLAN SELECT e.name, d.name FROM employees e INNER JOIN departments d ON e.dept_id = d.id"
        );

        boolean foundEmployees = false;
        boolean foundDepartments = false;
        for (int i = 0; i < result.getRowCount(); i++) {
            String detail = result.getValue(i, 3).asString();
            if (detail.contains("employees")) foundEmployees = true;
            if (detail.contains("departments")) foundDepartments = true;
        }
        assertTrue(foundEmployees, "Expected employees in plan");
        assertTrue(foundDepartments, "Expected departments in plan");
    }

    @Test
    void testExplainQueryPlanWithOrderByAndLimit() throws Exception {
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");

        Result result = db.execute("EXPLAIN QUERY PLAN SELECT name FROM items ORDER BY value DESC LIMIT 10");

        boolean foundOrderBy = false;
        boolean foundLimit = false;
        for (int i = 0; i < result.getRowCount(); i++) {
            String detail = result.getValue(i, 3).asString();
            if (detail.contains("ORDER BY")) foundOrderBy = true;
            if (detail.contains("LIMIT")) foundLimit = true;
        }
        assertTrue(foundOrderBy, "Expected ORDER BY in plan");
        assertTrue(foundLimit, "Expected LIMIT in plan");
    }

    @Test
    void testExplainQueryPlanInsert() throws Exception {
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)");

        Result result = db.execute("EXPLAIN QUERY PLAN INSERT INTO test (val) VALUES ('hello')");

        assertTrue(result.getRowCount() >= 1);
        String detail = result.getValue(0, 3).asString();
        assertTrue(detail.contains("INSERT INTO test"), "Expected INSERT INTO test, got: " + detail);
    }

    @Test
    void testExplainWithoutQueryPlan() throws Exception {
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)");

        Result result = db.execute("EXPLAIN SELECT * FROM test");

        assertArrayEquals(new String[]{"addr", "opcode", "p1", "p2", "p3", "p4", "comment"}, result.getColumnNames());
        assertTrue(result.getRowCount() >= 1);
    }

    @Test
    void testExplainQueryPlanUsesIndex() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, age INTEGER)");
        db.execute("INSERT INTO users (email, age) VALUES ('alice@test.com', 30)");
        db.execute("INSERT INTO users (email, age) VALUES ('bob@test.com', 25)");
        db.execute("CREATE INDEX idx_email ON users (email)");

        Result result = db.execute("EXPLAIN QUERY PLAN SELECT * FROM users WHERE email = 'alice@test.com'");

        boolean foundSearch = false;
        for (int i = 0; i < result.getRowCount(); i++) {
            String detail = result.getValue(i, 3).asString();
            if (detail.contains("SEARCH TABLE users") && detail.contains("USING INDEX idx_email")) {
                foundSearch = true;
            }
        }
        assertTrue(foundSearch, "Expected SEARCH TABLE users USING INDEX idx_email, got plan without index usage");
    }

    @Test
    void testExplainQueryPlanScanWhenNoIndex() throws Exception {
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, age INTEGER)");
        db.execute("INSERT INTO users (email, age) VALUES ('alice@test.com', 30)");

        Result result = db.execute("EXPLAIN QUERY PLAN SELECT * FROM users WHERE email = 'alice@test.com'");

        boolean foundScan = false;
        for (int i = 0; i < result.getRowCount(); i++) {
            String detail = result.getValue(i, 3).asString();
            if (detail.contains("SCAN TABLE users")) {
                foundScan = true;
            }
        }
        assertTrue(foundScan, "Expected SCAN TABLE users when no index exists");
    }

    @Test
    void testExplainQueryPlanCompositeIndex() throws Exception {
        db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, order_date TEXT)");
        db.execute("INSERT INTO orders (customer_id, order_date) VALUES (1, '2024-01-01')");
        db.execute("CREATE INDEX idx_cust_date ON orders (customer_id, order_date)");

        Result result = db.execute("EXPLAIN QUERY PLAN SELECT * FROM orders WHERE customer_id = 1 AND order_date = '2024-01-01'");

        boolean foundSearch = false;
        for (int i = 0; i < result.getRowCount(); i++) {
            String detail = result.getValue(i, 3).asString();
            if (detail.contains("SEARCH TABLE orders") && detail.contains("USING INDEX idx_cust_date")) {
                foundSearch = true;
            }
        }
        assertTrue(foundSearch, "Expected SEARCH with composite index");
    }

    @Test
    void testExplainQueryPlanIndexPartialMatch() throws Exception {
        db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, price REAL, name TEXT)");
        db.execute("INSERT INTO products (category, price, name) VALUES ('books', 9.99, 'Book A')");
        db.execute("CREATE INDEX idx_cat_price ON products (category, price)");

        Result result = db.execute("EXPLAIN QUERY PLAN SELECT * FROM products WHERE category = 'books'");

        boolean foundSearch = false;
        for (int i = 0; i < result.getRowCount(); i++) {
            String detail = result.getValue(i, 3).asString();
            if (detail.contains("SEARCH TABLE products") && detail.contains("USING INDEX idx_cat_price")) {
                foundSearch = true;
            }
        }
        assertTrue(foundSearch, "Expected SEARCH with partial composite index match");
    }

    @Test
    void testExplainQueryPlanUniqueIndexOnColumn() throws Exception {
        db.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, email TEXT)");
        db.execute("INSERT INTO accounts (email) VALUES ('alice@test.com')");
        db.execute("CREATE UNIQUE INDEX idx_accounts_email ON accounts (email)");

        Result result = db.execute("EXPLAIN QUERY PLAN SELECT * FROM accounts WHERE email = 'alice@test.com'");

        boolean foundSearch = false;
        for (int i = 0; i < result.getRowCount(); i++) {
            String detail = result.getValue(i, 3).asString();
            if (detail.contains("SEARCH TABLE accounts") && detail.contains("USING INDEX idx_accounts_email")) {
                foundSearch = true;
            }
        }
        assertTrue(foundSearch, "Expected SEARCH using unique index");
    }

    @Test
    void testExplainQueryPlanIndexWithJoin() throws Exception {
        db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)");
        db.execute("CREATE INDEX idx_dept ON employees (dept_id)");

        Result result = db.execute(
            "EXPLAIN QUERY PLAN SELECT e.name FROM employees e INNER JOIN departments d ON e.dept_id = d.id"
        );

        boolean foundSearch = false;
        boolean foundScan = false;
        for (int i = 0; i < result.getRowCount(); i++) {
            String detail = result.getValue(i, 3).asString();
            if (detail.contains("SEARCH TABLE employees") && detail.contains("USING INDEX idx_dept")) {
                foundSearch = true;
            }
            if (detail.contains("SCAN TABLE departments") || detail.contains("departments")) {
                foundScan = true;
            }
        }
        assertTrue(foundSearch, "Expected SEARCH on employees using index from join condition");
        assertTrue(foundScan, "Expected departments in plan");
    }

    @Test
    void testExplainQueryPlanViaJdbc() throws Exception {
        String path = tempDir.resolve("test-explain-jdbc.db").toString();
        new File(path).delete();

        try (Connection conn = DriverManager.getConnection("jdbc:jasqlite:" + path)) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.executeUpdate("INSERT INTO users (name) VALUES ('Alice')");
            stmt.executeUpdate("INSERT INTO users (name) VALUES ('Bob')");

            ResultSet rs = stmt.executeQuery("EXPLAIN QUERY PLAN SELECT * FROM users");

            assertEquals("selectid", rs.getMetaData().getColumnName(1));
            assertEquals("order", rs.getMetaData().getColumnName(2));
            assertEquals("from", rs.getMetaData().getColumnName(3));
            assertEquals("detail", rs.getMetaData().getColumnName(4));

            assertTrue(rs.next());
            String detail = rs.getString("detail");
            assertTrue(detail.contains("SCAN TABLE users"), "Expected SCAN TABLE users, got: " + detail);
        }
    }
}
