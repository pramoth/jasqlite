package com.jasqlite.store;

import com.jasqlite.function.FunctionRegistry;
import com.jasqlite.sql.parser.Lexer;
import com.jasqlite.sql.parser.SQLParser;
import com.jasqlite.sql.ast.*;
import com.jasqlite.sql.planner.QueryPlanner;
import com.jasqlite.store.btree.BTree;
import com.jasqlite.store.page.Pager;
import com.jasqlite.store.record.Record;
import com.jasqlite.util.SQLiteValue;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main database class - the facade for all database operations.
 * Manages connections, schema, and query execution.
 */
public class Database implements AutoCloseable {

    private final String path;
    private final Pager pager;
    private final BTree btree;
    private final FunctionRegistry functionRegistry;
    private final QueryPlanner queryPlanner;

    // Schema cache
    private final Map<String, TableInfo> tables;
    private final Map<String, IndexInfo> indexes;
    private final Map<String, SchemaEntry> schemaEntries;
    private int schemaCookie;
    private boolean schemaLoaded;

    // Transaction state
    private boolean inTransaction;
    private int transactionDepth;

    // Statistics
    private long lastInsertRowid;
    private int changesCount;

    private final Object lock = new Object();

    public Database(String path, boolean readOnly) throws SQLException {
        this.path = path;
        try {
            this.pager = new Pager(path, readOnly);
        } catch (IOException e) {
            throw new SQLException("Failed to open database: " + e.getMessage(), e);
        }
        this.btree = new BTree(pager);
        this.functionRegistry = new FunctionRegistry();
        this.queryPlanner = new QueryPlanner(this);
        this.tables = new ConcurrentHashMap<>();
        this.indexes = new ConcurrentHashMap<>();
        this.schemaEntries = new ConcurrentHashMap<>();
        this.schemaLoaded = false;
        this.inTransaction = false;
        this.transactionDepth = 0;
        this.lastInsertRowid = 0;
        this.changesCount = 0;
    }

    public Database(String path) throws SQLException {
        this(path, false);
    }

    /**
     * Load schema from sqlite_master (page 1).
     */
    public void loadSchema() throws SQLException {
        if (schemaLoaded && pager.getSchemaCookie() == schemaCookie) return;

        synchronized (lock) {
            tables.clear();
            indexes.clear();
            schemaEntries.clear();

            try {
                // Read sqlite_master entries from the root page (page 1)
                List<BTree.RecordWithRowid> rows = btree.scanTable(1);

                for (BTree.RecordWithRowid rwr : rows) {
                    Record record = rwr.record;
                    if (record.getColumnCount() < 5) continue;

                    String type = record.getValue(0).asString();
                    String name = record.getValue(1).asString();
                    String tblName = record.getValue(2).asString();
                    int rootpage = (int) record.getValue(3).asLong();
                    String sql = record.getValue(4).asString();

                    SchemaEntry entry = new SchemaEntry(type, name, tblName, rootpage, sql);
                    schemaEntries.put(name.toLowerCase(), entry);

                    if ("table".equalsIgnoreCase(type)) {
                        TableInfo tableInfo = parseTableInfo(entry);
                        tables.put(name.toLowerCase(), tableInfo);
                    } else if ("index".equalsIgnoreCase(type)) {
                        IndexInfo indexInfo = parseIndexInfo(entry);
                        indexes.put(name.toLowerCase(), indexInfo);
                    }
                }

                schemaCookie = pager.getSchemaCookie();
                schemaLoaded = true;
            } catch (IOException e) {
                throw new SQLException("Failed to load schema: " + e.getMessage(), e);
            }
        }
    }

    private TableInfo parseTableInfo(SchemaEntry entry) {
        TableInfo info = new TableInfo();
        info.name = entry.name;
        info.rootPage = entry.rootpage;
        info.sql = entry.sql;
        info.columns = new ArrayList<>();

        if (entry.sql != null) {
            try {
                SQLParser parser = SQLParser.create(entry.sql);
                Statement stmt = parser.parse();
                if (stmt instanceof CreateTableStatement) {
                    CreateTableStatement ct = (CreateTableStatement) stmt;
                    if (ct.columns != null) {
                        int cid = 0;
                        for (ColumnDefinition colDef : ct.columns) {
                            ColumnInfo ci = new ColumnInfo();
                            ci.name = colDef.name;
                            ci.type = colDef.typeName != null ? colDef.typeName.name : "";
                            ci.cid = cid++;

                            if (colDef.constraints != null) {
                                for (ColumnConstraint cc : colDef.constraints) {
                                    if (cc instanceof Constraints.PrimaryKeyColumnConstraint) {
                                        ci.primaryKey = true;
                                        ci.autoIncrement = ((Constraints.PrimaryKeyColumnConstraint) cc).autoIncrement;
                                    } else if (cc instanceof Constraints.NotNullConstraint) {
                                        ci.notNull = true;
                                    } else if (cc instanceof Constraints.UniqueColumnConstraint) {
                                        ci.unique = true;
                                    } else if (cc instanceof Constraints.DefaultColumnConstraint) {
                                        ci.defaultValue = ((Constraints.DefaultColumnConstraint) cc).expression != null
                                            ? ((Constraints.DefaultColumnConstraint) cc).expression.toString() : null;
                                    } else if (cc instanceof Constraints.CollateColumnConstraint) {
                                        ci.collation = ((Constraints.CollateColumnConstraint) cc).collationName;
                                    }
                                }
                            }
                            info.columns.add(ci);
                        }
                    }
                    // Check table-level constraints
                    if (ct.constraints != null) {
                        for (TableConstraint tc : ct.constraints) {
                            if (tc instanceof TableConstraint.PrimaryKey) {
                                TableConstraint.PrimaryKey pk = (TableConstraint.PrimaryKey) tc;
                                if (pk.columns != null && pk.columns.size() == 1) {
                                    // Mark the column as primary key
                                    for (ColumnInfo ci : info.columns) {
                                        IndexedColumn ic = pk.columns.get(0);
                                        if (ic.expression instanceof Expressions.ColumnRef) {
                                            if (ci.name.equalsIgnoreCase(((Expressions.ColumnRef) ic.expression).columnName)) {
                                                ci.primaryKey = true;
                                                ci.autoIncrement = pk.autoIncrement;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                // If parsing fails, create basic column info
            }
        }

        return info;
    }

    private IndexInfo parseIndexInfo(SchemaEntry entry) {
        IndexInfo info = new IndexInfo();
        info.name = entry.name;
        info.tableName = entry.tblName;
        info.rootPage = entry.rootpage;
        info.sql = entry.sql;
        info.columns = new ArrayList<>();
        info.descending = new ArrayList<>();
        if (entry.sql != null) {
            try {
                Statement stmt = SQLParser.create(entry.sql).parse();
                if (stmt instanceof CreateIndexStatement) {
                    CreateIndexStatement ci = (CreateIndexStatement) stmt;
                    info.unique = ci.unique;
                    if (ci.columns != null) {
                        for (IndexedColumn ic : ci.columns) {
                            if (ic.expression instanceof Expressions.ColumnRef) {
                                info.columns.add(((Expressions.ColumnRef) ic.expression).columnName);
                            } else if (ic.expression != null) {
                                info.columns.add(ic.expression.toString());
                            }
                            info.descending.add(ic.order == Enums.OrderDirection.DESC);
                        }
                    }
                }
            } catch (Exception ignored) {
            }
        }
        return info;
    }

    /**
     * Execute a SQL statement.
     */
    public Result execute(String sql) throws SQLException {
        return execute(sql, null);
    }

    /**
     * Execute a SQL statement with parameters.
     */
    public Result execute(String sql, SQLiteValue[] params) throws SQLException {
        loadSchema();

        try {
            SQLParser parser = SQLParser.create(sql);
            Statement stmt = parser.parse();
            return queryPlanner.execute(stmt, params);
        } catch (Exception e) {
            if (e instanceof SQLException) throw (SQLException) e;
            throw new SQLException(e.getMessage(), e);
        }
    }

    /**
     * Execute multiple SQL statements separated by semicolons.
     */
    public List<Result> executeAll(String sql) throws SQLException {
        loadSchema();
        List<Result> results = new ArrayList<>();

        try {
            SQLParser parser = SQLParser.create(sql);
            List<Statement> stmts = parser.parseAll();
            for (Statement stmt : stmts) {
                results.add(queryPlanner.execute(stmt, null));
            }
        } catch (Exception e) {
            if (e instanceof SQLException) throw (SQLException) e;
            throw new SQLException(e.getMessage(), e);
        }

        return results;
    }

    // ==================== Schema Modification ====================

    public void createTable(CreateTableStatement stmt) throws SQLException {
        synchronized (lock) {
            try {
                // Allocate a root page for the table
                int rootPage = btree.createTable();

                // Build the SQL text for sqlite_master
                String sql = reconstructCreateTableSQL(stmt);

                // Insert into sqlite_master
                Record masterRecord = new Record();
                masterRecord.addValue(SQLiteValue.fromText("table"));
                masterRecord.addValue(SQLiteValue.fromText(stmt.tableName));
                masterRecord.addValue(SQLiteValue.fromText(stmt.tableName));
                masterRecord.addValue(SQLiteValue.fromLong(rootPage));
                masterRecord.addValue(SQLiteValue.fromText(sql));

                long rowid = btree.nextRowid(1);
                btree.insert(1, rowid, masterRecord);

                // Update schema cookie
                pager.setSchemaCookie(pager.getSchemaCookie() + 1);
                updatePage1Header();

                // Commit
                pager.commit();

                // Update local cache
                SchemaEntry entry = new SchemaEntry("table", stmt.tableName, stmt.tableName, rootPage, sql);
                schemaEntries.put(stmt.tableName.toLowerCase(), entry);
                tables.put(stmt.tableName.toLowerCase(), parseTableInfo(entry));
                schemaCookie = pager.getSchemaCookie();

            } catch (IOException e) {
                try { pager.rollback(); } catch (IOException ignored) {}
                throw new SQLException("Failed to create table: " + e.getMessage(), e);
            }
        }
    }

    public void createIndex(CreateIndexStatement stmt) throws SQLException {
        synchronized (lock) {
            try {
                int rootPage = btree.createIndex();

                String sql = reconstructCreateIndexSQL(stmt);

                Record masterRecord = new Record();
                masterRecord.addValue(SQLiteValue.fromText("index"));
                masterRecord.addValue(SQLiteValue.fromText(stmt.indexName));
                masterRecord.addValue(SQLiteValue.fromText(stmt.tableName));
                masterRecord.addValue(SQLiteValue.fromLong(rootPage));
                masterRecord.addValue(SQLiteValue.fromText(sql));

                long rowid = btree.nextRowid(1);
                btree.insert(1, rowid, masterRecord);

                // Populate index from existing table data
                TableInfo table = tables.get(stmt.tableName.toLowerCase());
                if (table != null) {
                    populateIndex(table, stmt, rootPage);
                }

                pager.setSchemaCookie(pager.getSchemaCookie() + 1);
                updatePage1Header();
                pager.commit();

                SchemaEntry entry = new SchemaEntry("index", stmt.indexName, stmt.tableName, rootPage, sql);
                schemaEntries.put(stmt.indexName.toLowerCase(), entry);
                IndexInfo info = parseIndexInfo(entry);
                if (stmt.columns != null) {
                    for (IndexedColumn ic : stmt.columns) {
                        if (ic.expression instanceof Expressions.ColumnRef) {
                            info.columns.add(((Expressions.ColumnRef) ic.expression).columnName);
                        } else if (ic.expression != null) {
                            info.columns.add(ic.expression.toString());
                        }
                        info.descending.add(ic.order == Enums.OrderDirection.DESC);
                    }
                }
                info.unique = stmt.unique;
                indexes.put(stmt.indexName.toLowerCase(), info);
                schemaCookie = pager.getSchemaCookie();

            } catch (IOException e) {
                try { pager.rollback(); } catch (IOException ignored) {}
                throw new SQLException("Failed to create index: " + e.getMessage(), e);
            }
        }
    }

    private void populateIndex(TableInfo table, CreateIndexStatement stmt, int rootPage) throws IOException {
        List<BTree.RecordWithRowid> rows = btree.scanTable(table.rootPage);
        for (BTree.RecordWithRowid rwr : rows) {
            Record keyRecord = new Record();
            if (stmt.columns != null) {
                for (IndexedColumn ic : stmt.columns) {
                    if (ic.expression instanceof Expressions.ColumnRef) {
                        String colName = ((Expressions.ColumnRef) ic.expression).columnName;
                        int idx = table.getColumnIndex(colName);
                        if (idx >= 0) {
                            keyRecord.addValue(rwr.record.getValue(idx));
                        }
                    }
                }
            }
            keyRecord.addValue(SQLiteValue.fromLong(rwr.rowid));
            btree.insertIndex(rootPage, keyRecord.serialize());
        }
    }

    public void dropTable(String tableName) throws SQLException {
        synchronized (lock) {
            try {
                tableName = tableName.toLowerCase();
                SchemaEntry entry = schemaEntries.remove(tableName);
                if (entry == null) throw new SQLException("no such table: " + tableName);

                tables.remove(tableName);

                // Remove from sqlite_master - find and delete the row
                CellLocation loc = findSchemaEntry(tableName);
                if (loc != null) {
                    btree.delete(1, loc.rowid);
                }

                // Free the root page
                if (entry.rootpage > 1) {
                    pager.freePage(entry.rootpage);
                }

                pager.setSchemaCookie(pager.getSchemaCookie() + 1);
                updatePage1Header();
                pager.commit();
                schemaCookie = pager.getSchemaCookie();

            } catch (IOException e) {
                try { pager.rollback(); } catch (IOException ignored) {}
                throw new SQLException("Failed to drop table: " + e.getMessage(), e);
            }
        }
    }

    private static class CellLocation {
        long rowid;
        CellLocation(long rowid) { this.rowid = rowid; }
    }

    private CellLocation findSchemaEntry(String name) throws IOException {
        List<BTree.RecordWithRowid> rows = btree.scanTable(1);
        for (BTree.RecordWithRowid rwr : rows) {
            if (rwr.record.getColumnCount() >= 2) {
                String entryName = rwr.record.getValue(1).asString();
                if (name.equalsIgnoreCase(entryName)) {
                    return new CellLocation(rwr.rowid);
                }
            }
        }
        return null;
    }

    public void dropIndex(String indexName) throws SQLException {
        synchronized (lock) {
            try {
                indexName = indexName.toLowerCase();
                SchemaEntry entry = schemaEntries.remove(indexName);
                if (entry == null) throw new SQLException("no such index: " + indexName);
                indexes.remove(indexName);

                CellLocation loc = findSchemaEntry(indexName);
                if (loc != null) btree.delete(1, loc.rowid);

                if (entry.rootpage > 1) pager.freePage(entry.rootpage);

                pager.setSchemaCookie(pager.getSchemaCookie() + 1);
                updatePage1Header();
                pager.commit();
                schemaCookie = pager.getSchemaCookie();

            } catch (IOException e) {
                try { pager.rollback(); } catch (IOException ignored) {}
                throw new SQLException("Failed to drop index: " + e.getMessage(), e);
            }
        }
    }

    private void updatePage1Header() throws IOException {
        // The header is updated automatically when page 1 is written via the pager
    }

    // ==================== Data Operations ====================

    public long insertRow(String tableName, List<String> columns, List<SQLiteValue> values) throws SQLException {
        synchronized (lock) {
            try {
                TableInfo table = tables.get(tableName.toLowerCase());
                if (table == null) throw new SQLException("no such table: " + tableName);

                List<ColumnInfo> tableCols = table.columns != null ? table.columns : new ArrayList<>();

                // Find the INTEGER PRIMARY KEY column index (rowid alias)
                int pkColIndex = -1;
                for (int i = 0; i < tableCols.size(); i++) {
                    ColumnInfo ci = tableCols.get(i);
                    if (ci.primaryKey && "INTEGER".equalsIgnoreCase(ci.type)) {
                        pkColIndex = i;
                        break;
                    }
                }

                // Build the full row values array
                SQLiteValue[] rowValues = new SQLiteValue[tableCols.size()];

                if (columns == null || columns.isEmpty()) {
                    // Insert in column order
                    for (int i = 0; i < tableCols.size(); i++) {
                        if (i < values.size()) {
                            rowValues[i] = values.get(i);
                        } else {
                            ColumnInfo ci = tableCols.get(i);
                            if (ci.defaultValue != null) {
                                rowValues[i] = SQLiteValue.fromText(ci.defaultValue);
                            } else {
                                rowValues[i] = SQLiteValue.fromNull();
                            }
                        }
                    }
                } else {
                    // Map values to column positions
                    for (int i = 0; i < columns.size(); i++) {
                        int idx = table.getColumnIndex(columns.get(i));
                        if (idx < 0) throw new SQLException("table " + tableName + " has no column named " + columns.get(i));
                        if (i < values.size()) rowValues[idx] = values.get(i);
                    }
                    // Fill missing columns with defaults
                    for (int i = 0; i < tableCols.size(); i++) {
                        if (rowValues[i] == null) {
                            ColumnInfo ci = tableCols.get(i);
                            if (ci.defaultValue != null) {
                                rowValues[i] = SQLiteValue.fromText(ci.defaultValue);
                            } else {
                                rowValues[i] = SQLiteValue.fromNull();
                            }
                        }
                    }
                }

                // Build the record, EXCLUDING the INTEGER PRIMARY KEY column
                // (it's stored as the rowid, not in the record)
                Record record = new Record();
                for (int i = 0; i < tableCols.size(); i++) {
                    if (i != pkColIndex) {
                        record.addValue(rowValues[i]);
                    }
                }

                // Determine the rowid
                long rowid;
                if (pkColIndex >= 0 && rowValues[pkColIndex] != null && !rowValues[pkColIndex].isNull()) {
                    rowid = rowValues[pkColIndex].asLong();
                } else {
                    rowid = btree.nextRowid(table.rootPage);
                }

                pager.beginTransaction();
                btree.insert(table.rootPage, rowid, record);
                lastInsertRowid = rowid;
                changesCount++;
                pager.commit();

                return rowid;
            } catch (IOException e) {
                try { pager.rollback(); } catch (IOException ignored) {}
                throw new SQLException("Insert failed: " + e.getMessage(), e);
            }
        }
    }

    public int updateRows(String tableName, List<String> columns, List<SQLiteValue> values, String whereClause) throws SQLException {
        synchronized (lock) {
            try {
                TableInfo table = tables.get(tableName.toLowerCase());
                if (table == null) throw new SQLException("no such table: " + tableName);

                List<BTree.RecordWithRowid> rows = btree.scanTable(table.rootPage);
                int updated = 0;

                pager.beginTransaction();

                for (BTree.RecordWithRowid rwr : rows) {
                    // TODO: evaluate WHERE clause
                    // For now, update all rows if no where clause
                    Record newRecord = new Record();
                    List<ColumnInfo> tableCols = table.columns;

                    for (int i = 0; i < tableCols.size(); i++) {
                        int updateIdx = columns != null ? columns.indexOf(tableCols.get(i).name) : -1;
                        if (updateIdx >= 0 && updateIdx < values.size()) {
                            newRecord.addValue(values.get(updateIdx));
                        } else {
                            newRecord.addValue(rwr.record.getValue(i));
                        }
                    }

                    btree.delete(table.rootPage, rwr.rowid);
                    btree.insert(table.rootPage, rwr.rowid, newRecord);
                    updated++;
                }

                changesCount = updated;
                pager.commit();
                return updated;
            } catch (IOException e) {
                try { pager.rollback(); } catch (IOException ignored) {}
                throw new SQLException("Update failed: " + e.getMessage(), e);
            }
        }
    }

    public int deleteRows(String tableName, String whereClause) throws SQLException {
        synchronized (lock) {
            try {
                TableInfo table = tables.get(tableName.toLowerCase());
                if (table == null) throw new SQLException("no such table: " + tableName);

                List<BTree.RecordWithRowid> rows = btree.scanTable(table.rootPage);
                int deleted = 0;

                pager.beginTransaction();
                for (BTree.RecordWithRowid rwr : rows) {
                    // TODO: evaluate WHERE clause
                    btree.delete(table.rootPage, rwr.rowid);
                    deleted++;
                }

                changesCount = deleted;
                pager.commit();
                return deleted;
            } catch (IOException e) {
                try { pager.rollback(); } catch (IOException ignored) {}
                throw new SQLException("Delete failed: " + e.getMessage(), e);
            }
        }
    }

    // ==================== Transaction Management ====================

    public void beginTransaction() throws SQLException {
        synchronized (lock) {
            try {
                if (transactionDepth == 0) {
                    pager.beginTransaction();
                }
                transactionDepth++;
                inTransaction = true;
            } catch (IOException e) {
                throw new SQLException("Failed to begin transaction: " + e.getMessage(), e);
            }
        }
    }

    public void commitTransaction() throws SQLException {
        synchronized (lock) {
            try {
                transactionDepth--;
                if (transactionDepth <= 0) {
                    pager.commit();
                    transactionDepth = 0;
                    inTransaction = false;
                }
            } catch (IOException e) {
                throw new SQLException("Failed to commit: " + e.getMessage(), e);
            }
        }
    }

    public void rollbackTransaction() throws SQLException {
        synchronized (lock) {
            try {
                pager.rollback();
                transactionDepth = 0;
                inTransaction = false;
                schemaLoaded = false; // Reload schema
            } catch (IOException e) {
                throw new SQLException("Failed to rollback: " + e.getMessage(), e);
            }
        }
    }

    // ==================== Getters ====================

    public Pager getPager() { return pager; }
    public BTree getBTree() { return btree; }
    public FunctionRegistry getFunctionRegistry() { return functionRegistry; }
    public boolean isInTransaction() { return inTransaction; }
    public long getLastInsertRowid() { return lastInsertRowid; }
    public int getChangesCount() { return changesCount; }
    public void setChangesCount(int count) { this.changesCount = count; }

    public TableInfo getTable(String name) {
        return tables.get(name.toLowerCase());
    }

    public IndexInfo getIndex(String name) {
        return indexes.get(name.toLowerCase());
    }

    public Collection<TableInfo> getTables() { return tables.values(); }
    public Collection<IndexInfo> getIndexes() { return indexes.values(); }
    public Collection<SchemaEntry> getSchemaEntries() { return schemaEntries.values(); }

    // ==================== SQL Reconstruction ====================

    private String reconstructCreateTableSQL(CreateTableStatement stmt) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (stmt.temporary) sb.append("TEMPORARY ");
        sb.append("TABLE ");
        if (stmt.ifNotExists) sb.append("IF NOT EXISTS ");
        sb.append(quoteIdentifier(stmt.tableName));
        sb.append(" (");

        if (stmt.columns != null) {
            for (int i = 0; i < stmt.columns.size(); i++) {
                if (i > 0) sb.append(", ");
                ColumnDefinition col = stmt.columns.get(i);
                sb.append(quoteIdentifier(col.name));
                if (col.typeName != null) {
                    sb.append(" ").append(col.typeName.name);
                }
                if (col.constraints != null) {
                    for (ColumnConstraint cc : col.constraints) {
                        sb.append(" ");
                        if (cc instanceof Constraints.PrimaryKeyColumnConstraint) {
                            sb.append("PRIMARY KEY");
                            if (((Constraints.PrimaryKeyColumnConstraint) cc).autoIncrement) sb.append(" AUTOINCREMENT");
                        } else if (cc instanceof Constraints.NotNullConstraint) {
                            sb.append("NOT NULL");
                        } else if (cc instanceof Constraints.UniqueColumnConstraint) {
                            sb.append("UNIQUE");
                        } else if (cc instanceof Constraints.DefaultColumnConstraint) {
                            sb.append("DEFAULT ");
                            sb.append(((Constraints.DefaultColumnConstraint) cc).expression);
                        }
                    }
                }
            }
        }
        sb.append(")");
        return sb.toString();
    }

    private String reconstructCreateIndexSQL(CreateIndexStatement stmt) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (stmt.unique) sb.append("UNIQUE ");
        sb.append("INDEX ");
        if (stmt.ifNotExists) sb.append("IF NOT EXISTS ");
        sb.append(quoteIdentifier(stmt.indexName));
        sb.append(" ON ").append(quoteIdentifier(stmt.tableName));
        sb.append(" (");
        if (stmt.columns != null) {
            for (int i = 0; i < stmt.columns.size(); i++) {
                if (i > 0) sb.append(", ");
                IndexedColumn ic = stmt.columns.get(i);
                sb.append(ic.expression);
                if (ic.order == Enums.OrderDirection.DESC) sb.append(" DESC");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    private String quoteIdentifier(String id) {
        if (id == null) return "";
        if (id.matches("[a-zA-Z_][a-zA-Z0-9_]*")) return id;
        return "\"" + id.replace("\"", "\"\"") + "\"";
    }

    @Override
    public void close() throws SQLException {
        try {
            pager.close();
        } catch (IOException e) {
            throw new SQLException("Failed to close database: " + e.getMessage(), e);
        }
    }
}
