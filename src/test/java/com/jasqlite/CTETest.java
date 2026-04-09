package com.jasqlite;

import com.jasqlite.store.Result;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

class CTETest extends BaseTest {

    @Test
    @DisplayName("Simple CTE")
    void testSimpleCTE() throws Exception {
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)", null);
        db.execute("INSERT INTO t VALUES (1, 'Alice')", null);
        db.execute("INSERT INTO t VALUES (2, 'Bob')", null);
        db.execute("INSERT INTO t VALUES (3, 'Charlie')", null);

        Result r = db.execute("WITH cte AS (SELECT * FROM t WHERE id > 1) SELECT * FROM cte", null);
        assertEquals(2, r.getRowCount());
        assertEquals(2L, r.getValue(0, 0).asLong());
        assertEquals("Bob", r.getValue(0, 1).asString());
        assertEquals(3L, r.getValue(1, 0).asLong());
        assertEquals("Charlie", r.getValue(1, 1).asString());
    }

    @Test
    @DisplayName("CTE with explicit column names")
    void testCTEWithColumnNames() throws Exception {
        Result r = db.execute("WITH cte(x) AS (SELECT 1) SELECT x FROM cte", null);
        assertEquals(1, r.getRowCount());
        assertEquals(1L, r.getValue(0, 0).asLong());
        assertEquals("x", r.getColumnNames()[0]);
    }

    @Test
    @DisplayName("CTE with multiple columns")
    void testCTEMultipleColumns() throws Exception {
        Result r = db.execute("WITH cte(a, b) AS (SELECT 1, 'hello') SELECT a, b FROM cte", null);
        assertEquals(1, r.getRowCount());
        assertEquals(1L, r.getValue(0, 0).asLong());
        assertEquals("hello", r.getValue(0, 1).asString());
    }

    @Test
    @DisplayName("Multiple CTEs")
    void testMultipleCTEs() throws Exception {
        Result r = db.execute(
            "WITH cte1 AS (SELECT 1 AS x), cte2 AS (SELECT 2 AS y) SELECT x, y FROM cte1, cte2", null);
        assertEquals(1, r.getRowCount());
        assertEquals(1L, r.getValue(0, 0).asLong());
        assertEquals(2L, r.getValue(0, 1).asLong());
    }

    @Test
    @DisplayName("CTE referencing earlier CTE")
    void testCTEChaining() throws Exception {
        Result r = db.execute(
            "WITH cte1 AS (SELECT 1 AS x), cte2 AS (SELECT x + 1 AS y FROM cte1) SELECT y FROM cte2", null);
        assertEquals(1, r.getRowCount());
        assertEquals(2L, r.getValue(0, 0).asLong());
    }

    @Test
    @DisplayName("CTE with JOIN to real table")
    void testCTEJoinRealTable() throws Exception {
        db.execute("CREATE TABLE orders(id INTEGER PRIMARY KEY, amount REAL, customer_id INTEGER)", null);
        db.execute("INSERT INTO orders VALUES (1, 100.0, 10)", null);
        db.execute("INSERT INTO orders VALUES (2, 200.0, 20)", null);
        db.execute("INSERT INTO orders VALUES (3, 50.0, 10)", null);

        Result r = db.execute(
            "WITH big_orders AS (SELECT * FROM orders WHERE amount > 75) SELECT * FROM big_orders", null);
        assertEquals(2, r.getRowCount());
        assertEquals(100.0, r.getValue(0, 1).asDouble(), 0.01);
        assertEquals(200.0, r.getValue(1, 1).asDouble(), 0.01);
    }

    @Test
    @DisplayName("CTE with SELECT *")
    void testCTESelectStar() throws Exception {
        Result r = db.execute("WITH cte AS (SELECT 1 AS a, 2 AS b) SELECT * FROM cte", null);
        assertEquals(1, r.getRowCount());
        assertEquals(2, r.getColumnCount());
        assertEquals(1L, r.getValue(0, 0).asLong());
        assertEquals(2L, r.getValue(0, 1).asLong());
    }

    @Test
    @DisplayName("CTE used in subquery")
    void testCTEInSubquery() throws Exception {
        Result r = db.execute(
            "WITH cte AS (SELECT 1 AS x) SELECT * FROM cte WHERE x IN (SELECT x FROM cte)", null);
        assertEquals(1, r.getRowCount());
        assertEquals(1L, r.getValue(0, 0).asLong());
    }

    @Test
    @DisplayName("CTE with alias in FROM")
    void testCTEWithAlias() throws Exception {
        Result r = db.execute(
            "WITH cte AS (SELECT 1 AS x) SELECT c.x FROM cte c WHERE c.x = 1", null);
        assertEquals(1, r.getRowCount());
        assertEquals(1L, r.getValue(0, 0).asLong());
    }

    @Test
    @DisplayName("CTE with UNION ALL inside (non-recursive)")
    void testCTEWithUnionAll() throws Exception {
        Result r = db.execute(
            "WITH cte AS (SELECT 1 AS x UNION ALL SELECT 2) SELECT * FROM cte", null);
        assertEquals(2, r.getRowCount());
        assertEquals(1L, r.getValue(0, 0).asLong());
        assertEquals(2L, r.getValue(1, 0).asLong());
    }

    @Test
    @DisplayName("Recursive CTE - counting")
    void testRecursiveCTECounting() throws Exception {
        Result r = db.execute(
            "WITH RECURSIVE cnt(x) AS (" +
            "  SELECT 1" +
            "  UNION ALL" +
            "  SELECT x+1 FROM cnt WHERE x < 5" +
            ") SELECT * FROM cnt", null);
        assertEquals(5, r.getRowCount());
        for (int i = 0; i < 5; i++) {
            assertEquals((long) (i + 1), r.getValue(i, 0).asLong());
        }
    }

    @Test
    @DisplayName("Recursive CTE - factorial")
    void testRecursiveCTEFactorial() throws Exception {
        Result r = db.execute(
            "WITH RECURSIVE fact(n, f) AS (" +
            "  SELECT 1, 1" +
            "  UNION ALL" +
            "  SELECT n+1, f*(n+1) FROM fact WHERE n < 5" +
            ") SELECT * FROM fact", null);
        assertEquals(5, r.getRowCount());
        assertEquals(1L, r.getValue(0, 0).asLong());
        assertEquals(1L, r.getValue(0, 1).asLong());
        assertEquals(2L, r.getValue(1, 0).asLong());
        assertEquals(2L, r.getValue(1, 1).asLong());
        assertEquals(5L, r.getValue(4, 0).asLong());
        assertEquals(120L, r.getValue(4, 1).asLong());
    }

    @Test
    @DisplayName("Recursive CTE - Fibonacci")
    void testRecursiveCTEFibonacci() throws Exception {
        Result r = db.execute(
            "WITH RECURSIVE fib(n, a, b) AS (" +
            "  SELECT 1, 0, 1" +
            "  UNION ALL" +
            "  SELECT n+1, b, a+b FROM fib WHERE n < 8" +
            ") SELECT a FROM fib", null);
        assertEquals(8, r.getRowCount());
        long[] expected = {0, 1, 1, 2, 3, 5, 8, 13};
        for (int i = 0; i < 8; i++) {
            assertEquals(expected[i], r.getValue(i, 0).asLong());
        }
    }

    @Test
    @DisplayName("Recursive CTE - hierarchical data (org chart)")
    void testRecursiveCTEHierarchy() throws Exception {
        db.execute("CREATE TABLE emp(id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER)", null);
        db.execute("INSERT INTO emp VALUES (1, 'CEO', NULL)", null);
        db.execute("INSERT INTO emp VALUES (2, 'VP1', 1)", null);
        db.execute("INSERT INTO emp VALUES (3, 'VP2', 1)", null);
        db.execute("INSERT INTO emp VALUES (4, 'Mgr1', 2)", null);
        db.execute("INSERT INTO emp VALUES (5, 'Mgr2', 2)", null);
        db.execute("INSERT INTO emp VALUES (6, 'Dev1', 4)", null);

        Result r = db.execute(
            "WITH RECURSIVE chain(id, name, level) AS (" +
            "  SELECT id, name, 0 FROM emp WHERE manager_id IS NULL" +
            "  UNION ALL" +
            "  SELECT e.id, e.name, c.level + 1" +
            "  FROM emp e JOIN chain c ON e.manager_id = c.id" +
            ") SELECT name, level FROM chain", null);
        assertEquals(6, r.getRowCount());
        assertEquals("CEO", r.getValue(0, 0).asString());
        assertEquals(0L, r.getValue(0, 1).asLong());
    }

    @Test
    @DisplayName("Recursive CTE with no base rows returns empty")
    void testRecursiveCTEEmptyBase() throws Exception {
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)", null);
        Result r = db.execute(
            "WITH RECURSIVE cte(x) AS (" +
            "  SELECT id FROM t" +
            "  UNION ALL" +
            "  SELECT x+1 FROM cte WHERE x < 5" +
            ") SELECT * FROM cte", null);
        assertEquals(0, r.getRowCount());
    }

    @Test
    @DisplayName("Recursive CTE starting from table data")
    void testRecursiveCTEFromTable() throws Exception {
        db.execute("CREATE TABLE tree(id INTEGER PRIMARY KEY, parent_id INTEGER)", null);
        db.execute("INSERT INTO tree VALUES (1, NULL)", null);
        db.execute("INSERT INTO tree VALUES (2, 1)", null);
        db.execute("INSERT INTO tree VALUES (3, 1)", null);
        db.execute("INSERT INTO tree VALUES (4, 2)", null);
        db.execute("INSERT INTO tree VALUES (5, 2)", null);

        Result r = db.execute(
            "WITH RECURSIVE descendants(id, depth) AS (" +
            "  SELECT id, 0 FROM tree WHERE parent_id IS NULL" +
            "  UNION ALL" +
            "  SELECT t.id, d.depth + 1" +
            "  FROM tree t JOIN descendants d ON t.parent_id = d.id" +
            ") SELECT * FROM descendants", null);
        assertEquals(5, r.getRowCount());
    }

    @Test
    @DisplayName("Recursive CTE with string concatenation")
    void testRecursiveCTEStringConcat() throws Exception {
        Result r = db.execute(
            "WITH RECURSIVE letters(s, n) AS (" +
            "  SELECT 'A', 1" +
            "  UNION ALL" +
            "  SELECT s || 'A', n+1 FROM letters WHERE n < 5" +
            ") SELECT s, n FROM letters", null);
        assertEquals(5, r.getRowCount());
        assertEquals("A", r.getValue(0, 0).asString());
        assertEquals("AA", r.getValue(1, 0).asString());
        assertEquals("AAAAA", r.getValue(4, 0).asString());
    }

    @Test
    @DisplayName("CTE does not leak to subsequent statements")
    void testCTEDoesNotLeak() throws Exception {
        db.execute("WITH cte AS (SELECT 1 AS x) SELECT * FROM cte", null);
        assertThrows(Exception.class, () -> {
            db.execute("SELECT * FROM cte", null);
        });
    }

    @Test
    @DisplayName("UNION ALL basic")
    void testUnionAll() throws Exception {
        Result r = db.execute("SELECT 1 AS x UNION ALL SELECT 2", null);
        assertEquals(2, r.getRowCount());
        assertEquals(1L, r.getValue(0, 0).asLong());
        assertEquals(2L, r.getValue(1, 0).asLong());
    }

    @Test
    @DisplayName("UNION deduplicates")
    void testUnion() throws Exception {
        Result r = db.execute("SELECT 1 AS x UNION SELECT 1", null);
        assertEquals(1, r.getRowCount());
    }

    @Test
    @DisplayName("UNION ALL with table data")
    void testUnionAllWithTable() throws Exception {
        db.execute("CREATE TABLE t1(x INTEGER)", null);
        db.execute("CREATE TABLE t2(y INTEGER)", null);
        db.execute("INSERT INTO t1 VALUES (1)", null);
        db.execute("INSERT INTO t1 VALUES (2)", null);
        db.execute("INSERT INTO t2 VALUES (3)", null);
        db.execute("INSERT INTO t2 VALUES (4)", null);

        Result r = db.execute("SELECT x FROM t1 UNION ALL SELECT y FROM t2", null);
        assertEquals(4, r.getRowCount());
    }
}
