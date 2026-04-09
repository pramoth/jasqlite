package com.jasqlite;

import com.jasqlite.store.Database;
import com.jasqlite.store.Result;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class JoinTest extends BaseTest {

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

        assertEquals("Alice", result.getValue(0, 0).asString());
        assertEquals("Engineering", result.getValue(0, 1).asString());

        assertEquals("Bob", result.getValue(1, 0).asString());
        assertEquals("Marketing", result.getValue(1, 1).asString());

        assertEquals("Charlie", result.getValue(2, 0).asString());
        assertEquals("Engineering", result.getValue(2, 1).asString());

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

        db.execute("INSERT INTO orders (product, customer_id) VALUES ('Laptop', 1)");
        db.execute("INSERT INTO orders (product, customer_id) VALUES ('Phone', 1)");
        db.execute("INSERT INTO orders (product, customer_id) VALUES ('Tablet', 2)");

        Result result = db.execute(
            "SELECT c.name, o.product FROM customers c LEFT JOIN orders o ON c.id = o.customer_id ORDER BY c.name"
        );

        assertEquals(4, result.getRowCount());

        assertEquals("Alice", result.getValue(0, 0).asString());
        assertTrue("Laptop".equals(result.getValue(0, 1).asString()) ||
                   "Phone".equals(result.getValue(0, 1).asString()));

        assertEquals("Alice", result.getValue(1, 0).asString());
        assertTrue("Laptop".equals(result.getValue(1, 1).asString()) ||
                   "Phone".equals(result.getValue(1, 1).asString()));

        assertEquals("Bob", result.getValue(2, 0).asString());
        assertEquals("Tablet", result.getValue(2, 1).asString());

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
}
