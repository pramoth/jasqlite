# JaSQLite

A SQLite-compatible SQL engine written entirely in Java. JaSQLite reads and writes the native SQLite database file format, making it **binary compatible** with SQLite — databases created by the C library can be opened by JaSQLite and vice versa.

## Features

### Storage Engine
- **Binary-compatible file format** — 100-byte SQLite header, B-tree pages, varint encoding
- **B-tree storage** — table B-trees and index B-trees with leaf and interior pages, overflow page handling
- **Write-Ahead Log (WAL)** support
- **Page cache** with LRU eviction
- **Record format** — SQLite serial type codes for NULL, INTEGER, FLOAT, TEXT, and BLOB

### SQL Support
- `CREATE TABLE` / `DROP TABLE` with `IF NOT EXISTS` / `IF EXISTS`
- `INSERT`, `UPDATE`, `DELETE`
- `SELECT` with:
  - `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`
  - `GROUP BY` with `HAVING`
  - `DISTINCT`
  - Column aliases and table aliases
  - `INNER JOIN`, `LEFT JOIN`, multi-table joins with `ON` conditions
  - Subqueries (in `FROM`, `WHERE`, and expression positions)
  - `IN`, `BETWEEN`, `LIKE`, `GLOB`, `EXISTS`, `IS NULL`, `CASE WHEN`
  - `CAST` expressions
- `CREATE INDEX` / `DROP INDEX` (including `UNIQUE` indexes)
- `CREATE VIEW` / `DROP VIEW`
- `EXPLAIN` and `EXPLAIN QUERY PLAN` (with index usage reporting)
- Transactions: `BEGIN`, `COMMIT`, `ROLLBACK`
- `PRAGMA` (read and write)
- `ATTACH` / `DETACH` database

### Built-in Functions (80+)
| Category | Functions |
|----------|-----------|
| String | `upper`, `lower`, `length`, `substr`, `replace`, `trim`, `ltrim`, `rtrim`, `instr`, `printf`, `char`, `hex`, `unicode`, `quote`, `soundex`, `zeroblob`, `typeof` |
| Math | `abs`, `ceil`, `floor`, `log`, `log2`, `ln`, `exp`, `power`, `sqrt`, `sign`, `mod`, `trunc`, `pi`, `round`, `random` |
| Aggregate | `count`, `sum`, `total`, `avg`, `min`, `max`, `group_concat` |
| Date/Time | `date`, `time`, `datetime`, `julianday`, `strftime`, `unixepoch` |
| System | `coalesce`, `ifnull`, `iif`, `nullif`, `last_insert_rowid`, `changes` |

### JDBC Driver
Full JDBC implementation:
- `java.sql.Driver` — auto-registers via `Class.forName("com.jasqlite.jdbc.JaSQLiteDriver")`
- `Connection`, `Statement`, `PreparedStatement`, `ResultSet`
- `DatabaseMetaData` — tables, columns, indexes, primary keys
- Connection URL: `jdbc:jasqlite:/path/to/database.db`

### SQLite Compatibility
- `INTEGER PRIMARY KEY` as rowid alias
- `sqlite_master` / `sqlite_schema` virtual tables
- `?`, `?N`, `$name`, `:name`, `@name` parameter binding
- SQLite-compatible type affinity (NULL, INTEGER, REAL, TEXT, BLOB)

## Project Structure

```
src/main/java/com/jasqlite/
├── JaSQLite.java                  # Main entry point and shell
├── sql/
│   ├── parser/
│   │   ├── Lexer.java             # SQL tokenizer
│   │   ├── SQLParser.java         # Recursive descent parser
│   │   ├── Token.java             # Token representation
│   │   └── TokenType.java         # All SQLite keyword tokens
│   ├── ast/
│   │   ├── Statement.java         # Base statement AST
│   │   ├── SelectStatement.java
│   │   ├── InsertStatement.java
│   │   ├── Expressions.java       # All expression types
│   │   └── ... (35+ AST node files)
│   └── planner/
│       └── QueryPlanner.java      # Query execution engine
├── store/
│   ├── Database.java              # Database facade
│   ├── btree/BTree.java           # B-tree operations
│   ├── page/
│   │   ├── Page.java              # Page representation
│   │   ├── Pager.java             # Page I/O and caching
│   │   └── WALFile.java           # Write-Ahead Log
│   ├── record/Record.java         # Record serialization
│   ├── TableInfo.java, ColumnInfo.java, IndexInfo.java, SchemaEntry.java
│   └── Result.java                # Query result container
├── function/
│   └── FunctionRegistry.java      # 80+ built-in functions
├── jdbc/
│   ├── JaSQLiteDriver.java
│   ├── JaSQLiteConnection.java
│   ├── JaSQLiteStatement.java
│   ├── JaSQLitePreparedStatement.java
│   ├── JaSQLiteResultSet.java
│   ├── JaSQLiteResultSetMetaData.java
│   └── JaSQLiteDatabaseMetaData.java
├── vdbe/
│   ├── VDBE.java                  # Virtual Database Engine
│   ├── Opcode.java
│   ├── Instruction.java
│   └── Cursor.java
└── util/
    ├── SQLiteValue.java           # Value type system
    └── BinaryUtils.java           # Varint and binary utilities
```

## Getting Started

### Prerequisites
- Java 11 or later
- Maven 3.6+

### Build
```bash
mvn clean package
```

### Run Tests
```bash
mvn test
```

### Usage — Embedded API
```java
import com.jasqlite.store.Database;
import com.jasqlite.store.Result;

Database db = new Database("/path/to/my.db");
db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)");

Result result = db.execute("SELECT name, age FROM users WHERE age > 25");
for (int i = 0; i < result.getRowCount(); i++) {
    System.out.println(result.getValue(i, 0).asString() + ": " + result.getValue(i, 1).asLong());
}

db.close();
```

### Usage — JDBC
```java
import java.sql.*;

Class.forName("com.jasqlite.jdbc.JaSQLiteDriver");

try (Connection conn = DriverManager.getConnection("jdbc:jasqlite:/path/to/my.db")) {
    Statement stmt = conn.createStatement();
    stmt.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)");

    PreparedStatement ps = conn.prepareStatement("INSERT INTO products (name, price) VALUES (?, ?)");
    ps.setString(1, "Widget");
    ps.setDouble(2, 9.99);
    ps.executeUpdate();

    ResultSet rs = stmt.executeQuery("SELECT name, price FROM products");
    while (rs.next()) {
        System.out.println(rs.getString("name") + ": $" + rs.getDouble("price"));
    }
}
```

### Usage — Shell Mode
```bash
java -jar target/jasqlite-1.0.0.jar /path/to/my.db
```

### Usage — EXPLAIN QUERY PLAN
```java
// Without index — shows SCAN TABLE
Result plan = db.execute("EXPLAIN QUERY PLAN SELECT * FROM users WHERE email = 'alice@test.com'");
// detail: "SCAN TABLE users (~N rows)"

// With index — shows SEARCH TABLE ... USING INDEX
db.execute("CREATE INDEX idx_email ON users (email)");
Result plan = db.execute("EXPLAIN QUERY PLAN SELECT * FROM users WHERE email = 'alice@test.com'");
// detail: "SEARCH TABLE users USING INDEX idx_email (email=?)"

// Composite index — matches on prefix columns
db.execute("CREATE INDEX idx_cust_date ON orders (customer_id, order_date)");
Result plan = db.execute("EXPLAIN QUERY PLAN SELECT * FROM orders WHERE customer_id = 1");
// detail: "SEARCH TABLE orders USING INDEX idx_cust_date (customer_id=?)"

// Joins — uses index from ON condition
Result plan = db.execute(
    "EXPLAIN QUERY PLAN SELECT e.name FROM employees e INNER JOIN departments d ON e.dept_id = d.id");
// detail: "SEARCH TABLE employees USING INDEX idx_dept (dept_id=?)"
```

## Architecture

JaSQLite implements the core components of a relational database engine:

1. **Storage Layer** — Reads and writes the SQLite binary file format directly. Pages are managed through a pager with an LRU cache, journal-based rollback, and optional WAL mode.

2. **B-Tree Engine** — Implements table B-trees (clustered on rowid) and index B-trees. Handles leaf and interior pages, cell insertion/deletion, and page splitting.

3. **SQL Parser** — Hand-written recursive descent parser covering the full SQLite SQL grammar. Produces an AST with typed expression nodes.

4. **Query Planner** — Walks the AST and executes queries against the storage engine. Handles joins (via nested loop), aggregates, grouping, ordering, and filtering. `EXPLAIN QUERY PLAN` reports index usage via `SEARCH TABLE ... USING INDEX` when a matching index is found.

5. **VDBE** — Virtual Database Engine with an opcode-based instruction set, modeled after SQLite's VDBE.

6. **JDBC Driver** — Standard JDBC 4.2 implementation so JaSQLite can be used as a drop-in replacement for the SQLite JDBC driver in Java applications.

## License

This project is provided as-is for educational and research purposes.
