package com.jasqlite.jdbc;

import com.jasqlite.store.Database;
import com.jasqlite.store.SchemaEntry;
import com.jasqlite.store.TableInfo;
import com.jasqlite.store.ColumnInfo;
import com.jasqlite.store.IndexInfo;
import com.jasqlite.util.SQLiteValue;

import java.sql.*;
import java.util.*;

/**
 * DatabaseMetaData implementation for JaSQLite.
 */
public class JaSQLiteDatabaseMetaData implements DatabaseMetaData {

    private final JaSQLiteConnection conn;

    public JaSQLiteDatabaseMetaData(JaSQLiteConnection conn) {
        this.conn = conn;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException { return true; }
    @Override
    public boolean allTablesAreSelectable() throws SQLException { return true; }
    @Override
    public String getURL() throws SQLException { return ""; }
    @Override
    public String getUserName() throws SQLException { return ""; }
    @Override
    public boolean isReadOnly() throws SQLException { return false; }
    @Override
    public boolean nullsAreSortedHigh() throws SQLException { return true; }
    @Override
    public boolean nullsAreSortedLow() throws SQLException { return false; }
    @Override
    public boolean nullsAreSortedAtStart() throws SQLException { return false; }
    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException { return false; }
    @Override
    public String getDatabaseProductName() throws SQLException { return "JaSQLite"; }
    @Override
    public String getDatabaseProductVersion() throws SQLException { return "1.0.0"; }
    @Override
    public String getDriverName() throws SQLException { return "JaSQLite JDBC"; }
    @Override
    public String getDriverVersion() throws SQLException { return "1.0.0"; }
    @Override
    public int getDriverMajorVersion() { return 1; }
    @Override
    public int getDriverMinorVersion() { return 0; }
    @Override
    public boolean usesLocalFiles() throws SQLException { return true; }
    @Override
    public boolean usesLocalFilePerTable() throws SQLException { return false; }
    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException { return false; }
    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException { return false; }
    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException { return true; }
    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException { return true; }
    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException { return true; }
    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException { return false; }
    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException { return false; }
    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException { return true; }
    @Override
    public String getIdentifierQuoteString() throws SQLException { return "\""; }
    @Override
    public String getSQLKeywords() throws SQLException {
        return "ABORT,ACTION,AFTER,ANALYZE,ATTACH,AUTOINCREMENT,BEFORE,CASCADE,CONFLICT," +
               "DATABASE,DEFERRABLE,DEFERRED,DETACH,EXCLUSIVE,EXPLAIN,FAIL,GLOB,IGNORE," +
               "INDEXED,INITIALLY,INSTEAD,KEY,NOTHING,OFFSET,PLAN,PRAGMA,QUERY,RAISE," +
               "RECURSIVE,REGEXP,REINDEX,RENAME,REPLACE,RESTRICT,SAVEPOINT,TEMP,TRIGGER," +
               "VACUUM,VIEW,VIRTUAL,WITHOUT";
    }
    @Override
    public String getNumericFunctions() throws SQLException {
        return "abs,ceil,ceiling,floor,log,log2,ln,exp,power,sqrt,sign,mod,trunc,pi,round,random";
    }
    @Override
    public String getStringFunctions() throws SQLException {
        return "length,lower,upper,substr,substring,replace,trim,ltrim,rtrim,instr,printf,char,hex,unhex,unicode,quote,soundex,zeroblob,typeof";
    }
    @Override
    public String getSystemFunctions() throws SQLException {
        return "coalesce,ifnull,iif,nullif,likely,unlikely,changes,last_insert_rowid,typeof";
    }
    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "date,time,datetime,julianday,strftime,unixepoch";
    }
    @Override
    public String getSearchStringEscape() throws SQLException { return "\\"; }
    @Override
    public String getExtraNameCharacters() throws SQLException { return ""; }
    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException { return true; }
    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException { return true; }
    @Override
    public boolean supportsColumnAliasing() throws SQLException { return true; }
    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException { return true; }
    @Override
    public boolean supportsConvert() throws SQLException { return false; }
    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException { return false; }
    @Override
    public boolean supportsTableCorrelationNames() throws SQLException { return true; }
    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException { return false; }
    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException { return true; }
    @Override
    public boolean supportsOrderByUnrelated() throws SQLException { return true; }
    @Override
    public boolean supportsGroupBy() throws SQLException { return true; }
    @Override
    public boolean supportsGroupByUnrelated() throws SQLException { return true; }
    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException { return true; }
    @Override
    public boolean supportsLikeEscapeClause() throws SQLException { return true; }
    @Override
    public boolean supportsMultipleResultSets() throws SQLException { return false; }
    @Override
    public boolean supportsMultipleTransactions() throws SQLException { return true; }
    @Override
    public boolean supportsNonNullableColumns() throws SQLException { return true; }
    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException { return true; }
    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException { return true; }
    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException { return true; }
    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException { return true; }
    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException { return false; }
    @Override
    public boolean supportsANSI92FullSQL() throws SQLException { return false; }
    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException { return false; }
    @Override
    public boolean supportsOuterJoins() throws SQLException { return true; }
    @Override
    public boolean supportsFullOuterJoins() throws SQLException { return true; }
    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException { return true; }
    @Override
    public String getSchemaTerm() throws SQLException { return "schema"; }
    @Override
    public String getProcedureTerm() throws SQLException { return "procedure"; }
    @Override
    public String getCatalogTerm() throws SQLException { return "database"; }
    @Override
    public boolean isCatalogAtStart() throws SQLException { return true; }
    @Override
    public String getCatalogSeparator() throws SQLException { return "."; }
    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException { return false; }
    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException { return false; }
    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException { return false; }
    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException { return false; }
    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException { return false; }
    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException { return false; }
    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException { return false; }
    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException { return false; }
    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException { return false; }
    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException { return false; }
    @Override
    public boolean supportsPositionedDelete() throws SQLException { return false; }
    @Override
    public boolean supportsPositionedUpdate() throws SQLException { return false; }
    @Override
    public boolean supportsSelectForUpdate() throws SQLException { return false; }
    @Override
    public boolean supportsStoredProcedures() throws SQLException { return false; }
    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException { return true; }
    @Override
    public boolean supportsSubqueriesInExists() throws SQLException { return true; }
    @Override
    public boolean supportsSubqueriesInIns() throws SQLException { return true; }
    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException { return true; }
    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException { return true; }
    @Override
    public boolean supportsUnion() throws SQLException { return true; }
    @Override
    public boolean supportsUnionAll() throws SQLException { return true; }
    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException { return false; }
    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException { return false; }
    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException { return false; }
    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException { return false; }
    @Override
    public int getMaxBinaryLiteralLength() throws SQLException { return 1000000000; }
    @Override
    public int getMaxCharLiteralLength() throws SQLException { return 1000000000; }
    @Override
    public int getMaxColumnNameLength() throws SQLException { return 0; }
    @Override
    public int getMaxColumnsInGroupBy() throws SQLException { return 0; }
    @Override
    public int getMaxColumnsInIndex() throws SQLException { return 0; }
    @Override
    public int getMaxColumnsInOrderBy() throws SQLException { return 0; }
    @Override
    public int getMaxColumnsInSelect() throws SQLException { return 0; }
    @Override
    public int getMaxColumnsInTable() throws SQLException { return 2000; }
    @Override
    public int getMaxConnections() throws SQLException { return 0; }
    @Override
    public int getMaxCursorNameLength() throws SQLException { return 0; }
    @Override
    public int getMaxIndexLength() throws SQLException { return 0; }
    @Override
    public int getMaxSchemaNameLength() throws SQLException { return 0; }
    @Override
    public int getMaxProcedureNameLength() throws SQLException { return 0; }
    @Override
    public int getMaxCatalogNameLength() throws SQLException { return 0; }
    @Override
    public int getMaxRowSize() throws SQLException { return 1000000000; }
    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException { return false; }
    @Override
    public int getMaxStatementLength() throws SQLException { return 1000000; }
    @Override
    public int getMaxStatements() throws SQLException { return 0; }
    @Override
    public int getMaxTableNameLength() throws SQLException { return 0; }
    @Override
    public int getMaxTablesInSelect() throws SQLException { return 0; }
    @Override
    public int getMaxUserNameLength() throws SQLException { return 0; }
    @Override
    public int getDefaultTransactionIsolation() throws SQLException { return Connection.TRANSACTION_SERIALIZABLE; }
    @Override
    public boolean supportsTransactions() throws SQLException { return true; }
    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException { return true; }
    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException { return true; }
    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException { return false; }
    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException { return true; }
    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException { return false; }
    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        com.jasqlite.store.Result r = com.jasqlite.store.Result.query(
            new String[]{"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "", "", "", "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME"},
            new ArrayList<>()
        );
        return new JaSQLiteResultSet(r);
    }
    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[0], new ArrayList<>()));
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        Database db = conn.getDatabase();
        List<com.jasqlite.util.SQLiteValue[]> rows = new ArrayList<>();

        for (SchemaEntry entry : db.getSchemaEntries()) {
            if (types != null && types.length > 0) {
                boolean match = false;
                for (String type : types) {
                    if (type.equalsIgnoreCase(entry.type)) { match = true; break; }
                }
                if (!match) continue;
            }
            if (tableNamePattern != null && !tableNamePattern.isEmpty()) {
                if (!matchesPattern(entry.name, tableNamePattern)) continue;
            }
            rows.add(new SQLiteValue[]{
                SQLiteValue.fromNull(), SQLiteValue.fromNull(), SQLiteValue.fromText(entry.name),
                SQLiteValue.fromText(entry.type.toUpperCase()), SQLiteValue.fromNull(),
                SQLiteValue.fromNull(), SQLiteValue.fromNull(), SQLiteValue.fromNull(),
                SQLiteValue.fromNull(), SQLiteValue.fromText(entry.sql != null ? entry.sql : "")
            });
        }

        com.jasqlite.store.Result r = com.jasqlite.store.Result.query(
            new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"},
            rows
        );
        return new JaSQLiteResultSet(r);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        List<SQLiteValue[]> rows = new ArrayList<>();
        rows.add(new SQLiteValue[]{SQLiteValue.fromText("")});
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[]{"TABLE_CAT"}, rows));
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        List<SQLiteValue[]> rows = new ArrayList<>();
        rows.add(new SQLiteValue[]{SQLiteValue.fromText("TABLE")});
        rows.add(new SQLiteValue[]{SQLiteValue.fromText("VIEW")});
        rows.add(new SQLiteValue[]{SQLiteValue.fromText("SYSTEM TABLE")});
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[]{"TABLE_TYPE"}, rows));
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        Database db = conn.getDatabase();
        List<SQLiteValue[]> rows = new ArrayList<>();

        for (TableInfo table : db.getTables()) {
            if (tableNamePattern != null && !matchesPattern(table.name, tableNamePattern)) continue;
            if (table.columns != null) {
                for (int i = 0; i < table.columns.size(); i++) {
                    ColumnInfo col = table.columns.get(i);
                    if (columnNamePattern != null && !matchesPattern(col.name, columnNamePattern)) continue;
                    rows.add(new SQLiteValue[]{
                        SQLiteValue.fromNull(), SQLiteValue.fromNull(),
                        SQLiteValue.fromText(table.name), SQLiteValue.fromText(col.name),
                        SQLiteValue.fromLong(sqlTypeFromName(col.type)),
                        SQLiteValue.fromText(col.type != null ? col.type : ""),
                        SQLiteValue.fromLong(0), SQLiteValue.fromNull(),
                        SQLiteValue.fromNull(), SQLiteValue.fromLong(col.notNull ? DatabaseMetaData.columnNoNulls : DatabaseMetaData.columnNullable),
                        SQLiteValue.fromText(col.defaultValue != null ? col.defaultValue : ""),
                        SQLiteValue.fromNull(), SQLiteValue.fromNull(),
                        SQLiteValue.fromNull(), SQLiteValue.fromLong(i + 1),
                        SQLiteValue.fromText(col.notNull ? "NO" : "YES"),
                        SQLiteValue.fromNull(), SQLiteValue.fromNull(),
                        SQLiteValue.fromLong(0), SQLiteValue.fromLong(0),
                        SQLiteValue.fromLong(0), SQLiteValue.fromText("")
                    });
                }
            }
        }

        com.jasqlite.store.Result r = com.jasqlite.store.Result.query(
            new String[]{"TABLE_CAT","TABLE_SCHEM","TABLE_NAME","COLUMN_NAME","DATA_TYPE","TYPE_NAME","COLUMN_SIZE","BUFFER_LENGTH","DECIMAL_DIGITS","NUM_PREC_RADIX","NULLABLE","REMARKS","COLUMN_DEF","SQL_DATA_TYPE","SQL_DATETIME_SUB","CHAR_OCTET_LENGTH","ORDINAL_POSITION","IS_NULLABLE","SCOPE_CATALOG","SCOPE_SCHEMA","SCOPE_TABLE","SOURCE_DATA_TYPE"},
            rows
        );
        return new JaSQLiteResultSet(r);
    }

    private int sqlTypeFromName(String typeName) {
        if (typeName == null) return Types.VARCHAR;
        String t = typeName.toUpperCase();
        if (t.contains("INT")) return Types.BIGINT;
        if (t.contains("CHAR") || t.contains("CLOB") || t.contains("TEXT")) return Types.VARCHAR;
        if (t.contains("BLOB") || t.contains("BIN")) return Types.VARBINARY;
        if (t.contains("REAL") || t.contains("FLOA") || t.contains("DOUB")) return Types.DOUBLE;
        if (t.contains("NUM") || t.contains("DEC")) return Types.NUMERIC;
        if (t.contains("BOOL")) return Types.BOOLEAN;
        if (t.contains("DATE")) return Types.DATE;
        if (t.contains("TIME")) return Types.TIMESTAMP;
        return Types.VARCHAR;
    }

    private boolean matchesPattern(String value, String pattern) {
        if (pattern == null || pattern.isEmpty() || "%".equals(pattern)) return true;
        if (pattern.contains("%")) {
            String regex = pattern.replace("%", ".*").replace("_", ".");
            return value.matches("(?i)" + regex);
        }
        return value.equalsIgnoreCase(pattern);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[0], new ArrayList<>()));
    }
    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[0], new ArrayList<>()));
    }
    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[0], new ArrayList<>()));
    }
    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[0], new ArrayList<>()));
    }
    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        Database db = conn.getDatabase();
        List<SQLiteValue[]> rows = new ArrayList<>();
        TableInfo ti = db.getTable(table);
        if (ti != null && ti.columns != null) {
            int seq = 0;
            for (ColumnInfo col : ti.columns) {
                if (col.primaryKey) {
                    rows.add(new SQLiteValue[]{
                        SQLiteValue.fromNull(), SQLiteValue.fromNull(),
                        SQLiteValue.fromText(table), SQLiteValue.fromText(col.name),
                        SQLiteValue.fromLong(++seq), SQLiteValue.fromText("pk")
                    });
                }
            }
        }
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(
            new String[]{"TABLE_CAT","TABLE_SCHEM","TABLE_NAME","COLUMN_NAME","KEY_SEQ","PK_NAME"}, rows));
    }
    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[0], new ArrayList<>()));
    }
    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[0], new ArrayList<>()));
    }
    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[0], new ArrayList<>()));
    }
    @Override
    public ResultSet getTypeInfo() throws SQLException {
        List<SQLiteValue[]> rows = new ArrayList<>();
        String[][] types = {
            {"NULL", "0", "0", "", "1", "0", "12"}, {"INTEGER", "8", "0", "", "1", "0", "12"},
            {"REAL", "8", "0", "", "1", "0", "12"}, {"TEXT", "0", "0", "", "1", "0", "12"},
            {"BLOB", "0", "0", "", "1", "0", "12"}
        };
        for (String[] t : types) {
            rows.add(new SQLiteValue[]{
                SQLiteValue.fromText(t[0]), SQLiteValue.fromLong(Integer.parseInt(t[1])),
                SQLiteValue.fromLong(Integer.parseInt(t[2])), SQLiteValue.fromText(t[3]),
                SQLiteValue.fromNull(), SQLiteValue.fromText(t[5]),
                SQLiteValue.fromText(t[6]), SQLiteValue.fromNull(), SQLiteValue.fromNull(),
                SQLiteValue.fromNull(), SQLiteValue.fromNull()
            });
        }
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(
            new String[]{"TYPE_NAME","DATA_TYPE","PRECISION","LITERAL_PREFIX","LITERAL_SUFFIX","CREATE_PARAMS","NULLABLE","CASE_SENSITIVE","SEARCHABLE","UNSIGNED_ATTRIBUTE","FIXED_PREC_SCALE","AUTO_INCREMENT","LOCAL_TYPE_NAME","MINIMUM_SCALE","MAXIMUM_SCALE","SQL_DATA_TYPE","SQL_DATETIME_SUB","NUM_PREC_RADIX"}, rows));
    }
    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        Database db = conn.getDatabase();
        List<SQLiteValue[]> rows = new ArrayList<>();
        for (IndexInfo idx : db.getIndexes()) {
            if (table != null && !idx.tableName.equalsIgnoreCase(table)) continue;
            rows.add(new SQLiteValue[]{
                SQLiteValue.fromNull(), SQLiteValue.fromNull(),
                SQLiteValue.fromText(idx.tableName),
                SQLiteValue.fromText(idx.unique ? "false" : "true"),
                SQLiteValue.fromNull(),
                SQLiteValue.fromText(idx.name),
                SQLiteValue.fromLong(0),
                SQLiteValue.fromLong(1),
                SQLiteValue.fromText(idx.columns != null && !idx.columns.isEmpty() ? idx.columns.get(0) : ""),
                SQLiteValue.fromText("A"),
                SQLiteValue.fromLong(0),
                SQLiteValue.fromLong(0),
                SQLiteValue.fromNull()
            });
        }
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(
            new String[]{"TABLE_CAT","TABLE_SCHEM","TABLE_NAME","NON_UNIQUE","INDEX_QUALIFIER","INDEX_NAME","TYPE","ORDINAL_POSITION","COLUMN_NAME","ASC_OR_DESC","CARDINALITY","PAGES","FILTER_CONDITION"}, rows));
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException { return type == ResultSet.TYPE_FORWARD_ONLY; }
    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException { return concurrency == ResultSet.CONCUR_READ_ONLY; }
    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException { return false; }
    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException { return false; }
    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException { return false; }
    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException { return false; }
    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException { return false; }
    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException { return false; }
    @Override
    public boolean updatesAreDetected(int type) throws SQLException { return false; }
    @Override
    public boolean deletesAreDetected(int type) throws SQLException { return false; }
    @Override
    public boolean insertsAreDetected(int type) throws SQLException { return false; }
    @Override
    public boolean supportsBatchUpdates() throws SQLException { return true; }
    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[0], new ArrayList<>()));
    }
    @Override
    public Connection getConnection() throws SQLException { return conn; }
    @Override
    public boolean supportsSavepoints() throws SQLException { return false; }
    @Override
    public boolean supportsNamedParameters() throws SQLException { return false; }
    @Override
    public boolean supportsMultipleOpenResults() throws SQLException { return false; }
    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException { return true; }
    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[0], new ArrayList<>()));
    }
    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[0], new ArrayList<>()));
    }
    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[0], new ArrayList<>()));
    }
    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException { return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT; }
    @Override
    public int getResultSetHoldability() throws SQLException { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }
    @Override
    public int getDatabaseMajorVersion() throws SQLException { return 3; }
    @Override
    public int getDatabaseMinorVersion() throws SQLException { return 46; }
    @Override
    public int getJDBCMajorVersion() throws SQLException { return 4; }
    @Override
    public int getJDBCMinorVersion() throws SQLException { return 2; }
    @Override
    public int getSQLStateType() throws SQLException { return sqlStateSQL; }
    @Override
    public boolean locatorsUpdateCopy() throws SQLException { return true; }
    @Override
    public boolean supportsStatementPooling() throws SQLException { return false; }
    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException { return RowIdLifetime.ROWID_UNSUPPORTED; }
    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        List<SQLiteValue[]> rows = new ArrayList<>();
        rows.add(new SQLiteValue[]{SQLiteValue.fromText(""), SQLiteValue.fromText("")});
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[]{"TABLE_SCHEM", "TABLE_CATALOG"}, rows));
    }
    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException { return false; }
    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException { return false; }
    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[0], new ArrayList<>()));
    }
    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[0], new ArrayList<>()));
    }
    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[0], new ArrayList<>()));
    }
    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return new JaSQLiteResultSet(com.jasqlite.store.Result.query(new String[0], new ArrayList<>()));
    }
    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException { return true; }
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException { return iface.isInstance(this); }
}
