package com.jasqlite;

import com.jasqlite.store.Database;
import com.jasqlite.store.Result;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class ForeignKeyTest extends BaseTest {

    @Test
    void testForeignKeyRejectsInvalidChildInsert() throws Exception {
        db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("INSERT INTO departments (name) VALUES ('Engineering')");
        db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER REFERENCES departments(id))");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("INSERT INTO employees (name, dept_id) VALUES ('Alice', 999)"));
        assertTrue(ex.getMessage().contains("FOREIGN KEY constraint failed"),
            "Expected FK constraint failed, got: " + ex.getMessage());
    }

    @Test
    void testForeignKeyAllowsValidChildInsert() throws Exception {
        db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("INSERT INTO departments (name) VALUES ('Engineering')");
        db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER REFERENCES departments(id))");
        db.execute("INSERT INTO employees (name, dept_id) VALUES ('Alice', 1)");

        Result result = db.execute("SELECT name FROM employees");
        assertEquals(1, result.getRowCount());
        assertEquals("Alice", result.getValue(0, 0).asString());
    }

    @Test
    void testForeignKeyRejectsParentDelete() throws Exception {
        db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("INSERT INTO departments (name) VALUES ('Engineering')");
        db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER REFERENCES departments(id))");
        db.execute("INSERT INTO employees (name, dept_id) VALUES ('Alice', 1)");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("DELETE FROM departments"));
        assertTrue(ex.getMessage().contains("FOREIGN KEY constraint failed"),
            "Expected FK constraint failed, got: " + ex.getMessage());
    }

    @Test
    void testForeignKeyAllowsNullChildValue() throws Exception {
        db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER REFERENCES departments(id))");
        db.execute("INSERT INTO employees (name, dept_id) VALUES ('Alice', NULL)");

        Result result = db.execute("SELECT name FROM employees");
        assertEquals(1, result.getRowCount());
    }

    @Test
    void testForeignKeyTableLevelConstraint() throws Exception {
        db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, product_id INTEGER)");
        db.execute("INSERT INTO orders (customer_id, product_id) VALUES (1, 1)");
        db.execute("CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, quantity INTEGER, FOREIGN KEY(order_id) REFERENCES orders(id))");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("INSERT INTO order_items (order_id, quantity) VALUES (999, 5)"));
        assertTrue(ex.getMessage().contains("FOREIGN KEY constraint failed"),
            "Expected FK constraint failed, got: " + ex.getMessage());
    }

    @Test
    void testForeignKeyAllowsDeleteWhenNoChildren() throws Exception {
        db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
        db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER REFERENCES departments(id))");
        db.execute("INSERT INTO departments (name) VALUES ('Engineering')");
        db.execute("INSERT INTO departments (name) VALUES ('Sales')");
        db.execute("INSERT INTO employees (name, dept_id) VALUES ('Alice', 1)");

        SQLException ex = assertThrows(SQLException.class, () ->
            db.execute("DELETE FROM departments"));
        assertTrue(ex.getMessage().contains("FOREIGN KEY constraint failed"));

        db.execute("DELETE FROM employees");
        db.execute("DELETE FROM departments");

        Result result = db.execute("SELECT id FROM departments");
        assertEquals(0, result.getRowCount());
    }

    @Test
    void testForeignKeyViaJdbc() throws Exception {
        String path = tempDir.resolve("test-fk-jdbc.db").toString();
        new File(path).delete();

        try (Connection conn = DriverManager.getConnection("jdbc:jasqlite:" + path)) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.executeUpdate("INSERT INTO departments (name) VALUES ('Engineering')");
            stmt.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER REFERENCES departments(id))");

            SQLException ex = assertThrows(SQLException.class, () ->
                stmt.executeUpdate("INSERT INTO employees (name, dept_id) VALUES ('Alice', 999)"));
            assertTrue(ex.getMessage().contains("FOREIGN KEY constraint failed"));
        }
    }
}
