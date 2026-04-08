# JaSQLite

A SQLite-compatible SQL engine written in pure Java that is **binary compatible with SQLite database files**. JaSQLite can read and write SQLite 3 format `.db` files and exposes a standard JDBC interface, requiring no native dependencies.

**Compatible with SQLite 3.46.1**

## Features

- **Binary Compatibility** — Reads and writes the SQLite 3 file format directly
- **JDBC Interface** — Standard `java.sql` Driver, Connection, Statement, PreparedStatement, and ResultSet
- **Full SQL Support** — SELECT, INSERT, UPDATE, DELETE, CREATE/DROP TABLE/INDEX/VIEW/TRIGGER, ALTER TABLE
- **Query Capabilities** — WHERE, GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET, DISTINCT, subqueries
- **Joins** — INNER, LEFT, RIGHT, FULL, CROSS
- **50+ Built-in Functions** — String, math, date/time, aggregate, and type conversion functions
- **B-Tree Storage** — Table and index B-trees following the SQLite page format
- **Transactions** — BEGIN, COMMIT, ROLLBACK with journal-based recovery
- **Page Cache** — LRU page cache with configurable size
- **Interactive Shell** — Command-line REPL for executing SQL directly
- **Zero Native Dependencies** — Pure Java, runs anywhere Java 11+ is available

## Requirements

- Java 11 or later
- Maven 3.6+

## Build

```bash
mvn clean package
```

## Usage

### Interactive Shell

```bash
java -jar target/jasqlite-1.0.0.jar mydb.db
```

Shell commands: `.quit`, `.tables`, `.schema`

### JDBC

```java
import java.sql.*;

// Register driver (auto-registered via SPI)
Connection conn = DriverManager.getConnection("jdbc:jasqlite:mydb.db");
Statement stmt = conn.createStatement();

// Create a table
stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");

// Insert data
stmt.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')");

// Query
ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE name = 'Alice'");
while (rs.next()) {
    System.out.println(rs.getString("name") + " - " + rs.getString("email"));
}

conn.close();
```

### Programmatic API

```java
import com.jasqlite.JaSQLite;
import java.sql.Connection;

Connection conn = JaSQLite.createConnection("mydb.db");
// use conn as a standard JDBC connection
```

## Architecture

```
SQL String
  → Lexer → Tokens
  → SQLParser → AST
  → QueryPlanner → Storage Operations
  → BTree / Pager / Record (SQLite file I/O)
  → Result → JDBC ResultSet
```

| Layer | Package | Description |
|-------|---------|-------------|
| JDBC | `com.jasqlite.jdbc` | Standard java.sql interface implementation |
| SQL Parser | `com.jasqlite.sql.parser` | Recursive descent SQL parser and lexer |
| AST | `com.jasqlite.sql.ast` | Abstract syntax tree node classes |
| Query Planner | `com.jasqlite.sql.planner` | Query execution and optimization |
| Storage | `com.jasqlite.store` | Database, schema, and record management |
| B-Tree | `com.jasqlite.store.btree` | B-tree index and table storage |
| Pager | `com.jasqlite.store.page` | Page-level I/O, caching, and journaling |
| VDBE | `com.jasqlite.vdbe` | Virtual database engine (bytecode executor) |
| Functions | `com.jasqlite.function` | Built-in SQL function registry |
| Utilities | `com.jasqlite.util` | SQLite value types and binary encoding |

## Running Tests

```bash
mvn test
```

## License

This project is licensed under the [MIT License](LICENSE).
