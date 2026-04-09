package com.jasqlite.sql.planner;

import com.jasqlite.function.FunctionRegistry;
import com.jasqlite.sql.ast.*;
import com.jasqlite.sql.ast.Expressions.*;
import com.jasqlite.sql.ast.TableConstraint.*;
import com.jasqlite.sql.ast.TransactionStatements.*;
import com.jasqlite.sql.ast.DropStatements.*;
import com.jasqlite.sql.ast.Constraints.*;
import com.jasqlite.store.*;
import com.jasqlite.store.btree.BTree;
import com.jasqlite.store.record.Record;
import com.jasqlite.util.SQLiteValue;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class QueryPlanner {

    private final Database db;
    private int changesCount;

    public QueryPlanner(Database db) {
        this.db = db;
    }

    public Result execute(Statement stmt, SQLiteValue[] params) throws Exception {
        if (stmt instanceof SelectStatement) return executeSelect((SelectStatement) stmt, params);
        if (stmt instanceof InsertStatement) return executeInsert((InsertStatement) stmt, params);
        if (stmt instanceof UpdateStatement) return executeUpdate((UpdateStatement) stmt, params);
        if (stmt instanceof DeleteStatement) return executeDelete((DeleteStatement) stmt, params);
        if (stmt instanceof CreateTableStatement) return executeCreateTable((CreateTableStatement) stmt);
        if (stmt instanceof CreateIndexStatement) return executeCreateIndex((CreateIndexStatement) stmt);
        if (stmt instanceof CreateViewStatement) return executeCreateView((CreateViewStatement) stmt);
        if (stmt instanceof CreateTriggerStatement) return executeCreateTrigger((CreateTriggerStatement) stmt);
        if (stmt instanceof AlterTableStatement) return executeAlterTable((AlterTableStatement) stmt);
        if (stmt instanceof DropTable) return executeDropTable((DropTable) stmt);
        if (stmt instanceof DropIndex) return executeDropIndex((DropIndex) stmt);
        if (stmt instanceof DropView) return executeDropView((DropView) stmt);
        if (stmt instanceof DropTrigger) return executeDropTrigger((DropTrigger) stmt);
        if (stmt instanceof Begin) return executeBegin((Begin) stmt);
        if (stmt instanceof Commit) return executeCommit();
        if (stmt instanceof Rollback) return executeRollback();
        if (stmt instanceof Savepoint) return executeSavepoint((Savepoint) stmt);
        if (stmt instanceof PragmaStatement) return executePragma((PragmaStatement) stmt);
        if (stmt instanceof VacuumStatement) return executeVacuum();
        if (stmt instanceof ReindexStatement) return executeReindex((ReindexStatement) stmt);
        if (stmt instanceof AttachStatement) return executeAttach((AttachStatement) stmt);
        if (stmt instanceof DetachStatement) return executeDetach((DetachStatement) stmt);
        if (stmt instanceof ExplainStatement) return executeExplain((ExplainStatement) stmt);
        throw new Exception("Unsupported statement type: " + stmt.getClass().getSimpleName());
    }

    // ==================== SELECT ====================

    private Result executeSelect(SelectStatement sel, SQLiteValue[] params) throws Exception {
        // Handle CTEs (WITH clause) - store as temporary views
        // For now, handle basic SELECT

        if (sel.from == null || sel.from.isEmpty()) {
            // SELECT without FROM (e.g., SELECT 1+2)
            return executeSelectWithoutFrom(sel, params);
        }

        // Get rows from first table
        List<SQLiteValue[]> rows = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();

        // Process FROM clause
        TableOrSubquery from = sel.from.get(0);
        List<TableRow> tableRows;

        if (from.type == TableOrSubquery.Type.JOIN) {
            // First element is a JOIN - resolve recursively
            tableRows = resolveJoinTree(from, params);
        } else {
            tableRows = resolveTable(from, params);
        }

        // Process additional JOINs
        for (int i = 1; i < sel.from.size(); i++) {
            TableOrSubquery joinClause = sel.from.get(i);
            if (joinClause.type == TableOrSubquery.Type.JOIN && joinClause.left != null) {
                List<TableRow> rightRows = resolveTable(joinClause.right, params);
                tableRows = executeJoin(tableRows, rightRows, joinClause, params);
            } else if (joinClause.type == TableOrSubquery.Type.TABLE || joinClause.type == TableOrSubquery.Type.SUBQUERY) {
                // Cross join (comma-separated FROM)
                List<TableRow> rightRows = resolveTable(joinClause, params);
                // Cross join: cartesian product
                List<TableRow> crossResult = new ArrayList<>();
                for (TableRow left : tableRows) {
                    for (TableRow right : rightRows) {
                        crossResult.add(combineRows(left, right));
                    }
                }
                tableRows = crossResult;
            }
        }

        // Resolve column names from result columns
        List<String> resolvedNames = resolveColumnNames(sel.columns, from);

        // Apply WHERE filter
        if (sel.where != null) {
            List<TableRow> filtered = new ArrayList<>();
            for (TableRow tr : tableRows) {
                SQLiteValue result = evaluateExpression(sel.where, tr, params);
                if (result != null && !result.isNull() && result.asLong() != 0) {
                    filtered.add(tr);
                }
            }
            tableRows = filtered;
        }

        // Apply GROUP BY
        if (sel.groupBy != null && !sel.groupBy.isEmpty()) {
            Map<String, List<TableRow>> groups = new LinkedHashMap<>();
            for (TableRow tr : tableRows) {
                StringBuilder key = new StringBuilder();
                for (Expression gb : sel.groupBy) {
                    SQLiteValue val = evaluateExpression(gb, tr, params);
                    key.append(val != null ? val.toString() : "NULL").append("|");
                }
                groups.computeIfAbsent(key.toString(), k -> new ArrayList<>()).add(tr);
            }

            // Evaluate result columns for each group (with aggregate support)
            List<TableRow> groupedRows = new ArrayList<>();
            for (Map.Entry<String, List<TableRow>> entry : groups.entrySet()) {
                TableRow groupRow = createGroupRow(entry.getValue(), from);
                groupedRows.add(groupRow);
            }
            tableRows = groupedRows;

            // Apply HAVING
            if (sel.having != null) {
                List<TableRow> havingFiltered = new ArrayList<>();
                for (TableRow tr : tableRows) {
                    SQLiteValue result = evaluateExpression(sel.having, tr, params);
                    if (result != null && !result.isNull() && result.asLong() != 0) {
                        havingFiltered.add(tr);
                    }
                }
                tableRows = havingFiltered;
            }
        }

        // Evaluate result columns
        for (TableRow tr : tableRows) {
            List<SQLiteValue> rowValues = new ArrayList<>();
            for (ResultColumn rc : sel.columns) {
                if (rc.isStar) {
                    // SELECT *
                    rowValues.addAll(Arrays.asList(tr.values));
                } else if (rc.starTableName != null) {
                    // SELECT t.*
                    rowValues.addAll(Arrays.asList(tr.values));
                } else {
                    SQLiteValue val = evaluateExpression(rc.expression, tr, params);
                    rowValues.add(val != null ? val : SQLiteValue.fromNull());
                }
            }
            rows.add(rowValues.toArray(new SQLiteValue[0]));
        }

        // Apply DISTINCT
        if (sel.distinct && !rows.isEmpty()) {
            Set<String> seen = new LinkedHashSet<>();
            List<SQLiteValue[]> distinctRows = new ArrayList<>();
            for (SQLiteValue[] row : rows) {
                StringBuilder key = new StringBuilder();
                for (SQLiteValue v : row) key.append(v != null ? v.toString() : "NULL").append("|");
                if (seen.add(key.toString())) distinctRows.add(row);
            }
            rows = distinctRows;
        }

        // Apply ORDER BY
        if (sel.orderBy != null && !sel.orderBy.isEmpty()) {
            List<TableRow> orderRows = new ArrayList<>();
            for (SQLiteValue[] row : rows) {
                TableRow tr = new TableRow();
                tr.values = row;
                tr.tableName = "";
                tr.columnNames = resolvedNames != null ? resolvedNames : new ArrayList<>();
                orderRows.add(tr);
            }

            for (int oi = sel.orderBy.size() - 1; oi >= 0; oi--) {
                OrderByItem ob = sel.orderBy.get(oi);
                boolean desc = ob.direction == Enums.OrderDirection.DESC;
                orderRows.sort((a, b) -> {
                    SQLiteValue va = evaluateExpression(ob.expression, a, params);
                    SQLiteValue vb = evaluateExpression(ob.expression, b, params);
                    if (va == null || vb == null) return 0;
                    int cmp = va.compareTo(vb);
                    return desc ? -cmp : cmp;
                });
            }
            rows = new ArrayList<>();
            for (TableRow tr : orderRows) rows.add(tr.values);
        }

        // Apply LIMIT and OFFSET
        if (sel.limit != null) {
            int limit = (int) evaluateExpression(sel.limit, null, params).asLong();
            int offset = sel.offset != null ? (int) evaluateExpression(sel.offset, null, params).asLong() : 0;
            if (offset > 0 && offset < rows.size()) {
                rows = rows.subList(offset, rows.size());
            } else if (offset >= rows.size()) {
                rows = new ArrayList<>();
            }
            if (limit >= 0 && limit < rows.size()) {
                rows = new ArrayList<>(rows.subList(0, limit));
            }
        }

        // Resolve column names
        if (resolvedNames == null || resolvedNames.isEmpty()) {
            resolvedNames = resolveColumnNames(sel.columns, from);
        }

        return Result.query(resolvedNames.toArray(new String[0]), rows);
    }

    private Result executeSelectWithoutFrom(SelectStatement sel, SQLiteValue[] params) {
        List<SQLiteValue> rowValues = new ArrayList<>();
        List<String> names = new ArrayList<>();

        for (ResultColumn rc : sel.columns) {
            SQLiteValue val = evaluateExpression(rc.expression, null, params);
            rowValues.add(val != null ? val : SQLiteValue.fromNull());
            names.add(rc.alias != null ? rc.alias : (rc.expression != null ? rc.expression.toString() : "col"));
        }

        List<SQLiteValue[]> rows = new ArrayList<>();
        rows.add(rowValues.toArray(new SQLiteValue[0]));
        return Result.query(names.toArray(new String[0]), rows);
    }

    private List<TableRow> resolveJoinTree(TableOrSubquery join, SQLiteValue[] params) throws Exception {
        // Recursively resolve a tree of JOINs
        List<TableRow> leftRows;
        if (join.left.type == TableOrSubquery.Type.JOIN) {
            leftRows = resolveJoinTree(join.left, params);
        } else {
            leftRows = resolveTable(join.left, params);
        }
        List<TableRow> rightRows = resolveTable(join.right, params);
        return executeJoin(leftRows, rightRows, join, params);
    }

    private List<TableRow> resolveTable(TableOrSubquery tos, SQLiteValue[] params) throws Exception {
        if (tos.type == TableOrSubquery.Type.TABLE) {
            // Handle sqlite_master / sqlite_schema as a virtual table
            String tblName = tos.tableName.toLowerCase();
            if ("sqlite_master".equals(tblName) || "sqlite_schema".equals(tblName)) {
                return resolveSqliteMaster(tos.alias != null ? tos.alias : tos.tableName);
            }

            TableInfo table = db.getTable(tos.tableName);
            if (table == null) throw new Exception("no such table: " + tos.tableName);

            List<BTree.RecordWithRowid> records;
            try {
                records = db.getBTree().scanTable(table.rootPage);
            } catch (IOException e) {
                throw new Exception("Failed to scan table: " + e.getMessage(), e);
            }

            List<TableRow> rows = new ArrayList<>();
            List<String> colNames = new ArrayList<>();
            int pkColIndex = -1; // column index of INTEGER PRIMARY KEY (rowid alias)
            if (table.columns != null) {
                for (int ci = 0; ci < table.columns.size(); ci++) {
                    ColumnInfo col = table.columns.get(ci);
                    colNames.add(col.name);
                    if (col.primaryKey && "INTEGER".equalsIgnoreCase(col.type)) {
                        pkColIndex = ci;
                    }
                }
            }

            for (BTree.RecordWithRowid rwr : records) {
                TableRow tr = new TableRow();
                String tblAlias = tos.alias != null ? tos.alias : tos.tableName;
                tr.tableName = tblAlias;
                tr.columnNames = colNames;
                tr.tableNames = new ArrayList<>();
                for (int ci = 0; ci < colNames.size(); ci++) tr.tableNames.add(tblAlias);
                tr.rowid = rwr.rowid;

                // Build values array - if there's an INTEGER PRIMARY KEY, it's stored
                // as the rowid, not in the record. We need to splice it in.
                if (pkColIndex >= 0) {
                    SQLiteValue[] vals = new SQLiteValue[colNames.size()];
                    int recIdx = 0;
                    for (int ci = 0; ci < colNames.size(); ci++) {
                        if (ci == pkColIndex) {
                            vals[ci] = SQLiteValue.fromLong(rwr.rowid);
                        } else {
                            vals[ci] = recIdx < rwr.record.getColumnCount() ?
                                rwr.record.getValue(recIdx) : SQLiteValue.fromNull();
                            recIdx++;
                        }
                    }
                    tr.values = vals;
                } else {
                    SQLiteValue[] vals = new SQLiteValue[rwr.record.getColumnCount()];
                    for (int i = 0; i < vals.length; i++) {
                        vals[i] = rwr.record.getValue(i);
                    }
                    tr.values = vals;
                }
                rows.add(tr);
            }
            return rows;

        } else if (tos.type == TableOrSubquery.Type.SUBQUERY && tos.subquery != null) {
            Result subResult = executeSelect(tos.subquery, params);
            List<TableRow> rows = new ArrayList<>();
            List<String> colNames = new ArrayList<>();
            if (subResult.getColumnNames() != null) {
                colNames = Arrays.asList(subResult.getColumnNames());
            }

            for (int i = 0; i < subResult.getRowCount(); i++) {
                TableRow tr = new TableRow();
                tr.tableName = tos.alias != null ? tos.alias : "subquery";
                tr.columnNames = colNames;
                SQLiteValue[] row = new SQLiteValue[subResult.getColumnCount()];
                for (int j = 0; j < row.length; j++) {
                    row[j] = subResult.getValue(i, j);
                }
                tr.values = row;
                rows.add(tr);
            }
            return rows;

        } else if (tos.type == TableOrSubquery.Type.FUNCTION && tos.tableFunction != null) {
            // Table-valued function
            SQLiteValue fnResult = db.getFunctionRegistry().call(tos.tableFunction.name,
                tos.tableFunction.arguments != null ?
                    tos.tableFunction.arguments.toArray(new SQLiteValue[0]) : new SQLiteValue[0]);
            TableRow tr = new TableRow();
            tr.tableName = tos.alias != null ? tos.alias : tos.tableFunction.name;
            tr.columnNames = Arrays.asList(tos.tableFunction.name);
            tr.values = new SQLiteValue[]{fnResult != null ? fnResult : SQLiteValue.fromNull()};
            return Arrays.asList(tr);
        }

        return new ArrayList<>();
    }

    private List<TableRow> resolveSqliteMaster(String alias) throws Exception {
        List<TableRow> rows = new ArrayList<>();
        List<String> colNames = Arrays.asList("type", "name", "tbl_name", "rootpage", "sql");

        for (SchemaEntry entry : db.getSchemaEntries()) {
            TableRow tr = new TableRow();
            tr.tableName = alias;
            tr.columnNames = colNames;
            tr.tableNames = new ArrayList<>();
            for (int i = 0; i < colNames.size(); i++) tr.tableNames.add(alias);
            tr.values = new SQLiteValue[]{
                SQLiteValue.fromText(entry.type),
                SQLiteValue.fromText(entry.name),
                SQLiteValue.fromText(entry.tblName),
                SQLiteValue.fromLong(entry.rootpage),
                SQLiteValue.fromText(entry.sql != null ? entry.sql : "")
            };
            rows.add(tr);
        }
        return rows;
    }

    private List<TableRow> executeJoin(List<TableRow> left, List<TableRow> right, TableOrSubquery join, SQLiteValue[] params) {
        List<TableRow> result = new ArrayList<>();
        Enums.JoinType joinType = join.joinType != null ? join.joinType : Enums.JoinType.INNER;

        for (TableRow lr : left) {
            boolean matched = false;
            for (TableRow rr : right) {
                TableRow combined = combineRows(lr, rr);
                if (join.onCondition != null) {
                    SQLiteValue cond = evaluateExpression(join.onCondition, combined, params);
                    if (cond != null && !cond.isNull() && cond.asLong() != 0) {
                        result.add(combined);
                        matched = true;
                    }
                } else {
                    result.add(combined);
                    matched = true;
                }
            }
            // LEFT JOIN: add null-complemented row if no match
            if (!matched && (joinType == Enums.JoinType.LEFT || joinType == Enums.JoinType.NATURAL_LEFT)) {
                TableRow nullRight = createNullRow(right.isEmpty() ? new TableRow() : right.get(0));
                result.add(combineRows(lr, nullRight));
            }
        }

        return result;
    }

    private TableRow combineRows(TableRow left, TableRow right) {
        TableRow combined = new TableRow();
        List<String> names = new ArrayList<>();
        names.addAll(left.columnNames);
        names.addAll(right.columnNames);
        combined.columnNames = names;

        List<String> tnames = new ArrayList<>();
        tnames.addAll(left.tableNames);
        tnames.addAll(right.tableNames);
        combined.tableNames = tnames;

        SQLiteValue[] vals = new SQLiteValue[left.values.length + right.values.length];
        System.arraycopy(left.values, 0, vals, 0, left.values.length);
        System.arraycopy(right.values, 0, vals, left.values.length, right.values.length);
        combined.values = vals;
        combined.tableName = left.tableName;
        combined.rowid = left.rowid;
        return combined;
    }

    private TableRow createNullRow(TableRow template) {
        TableRow nullRow = new TableRow();
        nullRow.columnNames = template.columnNames;
        nullRow.values = new SQLiteValue[template.values.length];
        Arrays.fill(nullRow.values, SQLiteValue.fromNull());
        return nullRow;
    }

    private TableRow createGroupRow(List<TableRow> group, TableOrSubquery from) {
        if (group.isEmpty()) return new TableRow();
        TableRow first = group.get(0);
        TableRow gr = new TableRow();
        gr.columnNames = first.columnNames;
        gr.values = first.values;
        gr.tableName = first.tableName;
        gr.groupRows = group;
        return gr;
    }

    private List<String> resolveColumnNames(List<ResultColumn> columns, TableOrSubquery from) {
        List<String> names = new ArrayList<>();
        if (columns == null) return names;

        for (ResultColumn rc : columns) {
            if (rc.isStar) {
                if (from != null) {
                    TableInfo table = db.getTable(from.tableName);
                    if (table != null && table.columns != null) {
                        for (ColumnInfo ci : table.columns) names.add(ci.name);
                    }
                }
            } else if (rc.starTableName != null) {
                TableInfo table = db.getTable(from != null ? from.tableName : rc.starTableName);
                if (table != null && table.columns != null) {
                    for (ColumnInfo ci : table.columns) names.add(ci.name);
                }
            } else {
                names.add(rc.alias != null ? rc.alias :
                    (rc.expression instanceof Expressions.ColumnRef) ?
                        ((Expressions.ColumnRef) rc.expression).columnName :
                        rc.expression != null ? rc.expression.toString() : "col");
            }
        }
        return names;
    }

    // ==================== Expression Evaluation ====================

    public SQLiteValue evaluateExpression(Expression expr, TableRow row, SQLiteValue[] params) {
        if (expr == null) return SQLiteValue.fromNull();

        if (expr instanceof Expressions.Literal) {
            Expressions.Literal lit = (Expressions.Literal) expr;
            switch (lit.type) {
                case NULL: return SQLiteValue.fromNull();
                case INTEGER: return SQLiteValue.fromLong(Long.parseLong(lit.value));
                case FLOAT: return SQLiteValue.fromDouble(Double.parseDouble(lit.value));
                case STRING: return SQLiteValue.fromText(lit.value);
                case BLOB: return SQLiteValue.fromBlob(parseHexBlob(lit.value));
                case CURRENT_DATE: return SQLiteValue.fromText(java.time.LocalDate.now().toString());
                case CURRENT_TIME: return SQLiteValue.fromText(java.time.LocalTime.now().toString().substring(0, 8));
                case CURRENT_TIMESTAMP: return SQLiteValue.fromText(java.time.LocalDateTime.now().toString().replace('T', ' ').substring(0, 19));
                default: return SQLiteValue.fromNull();
            }
        }

        if (expr instanceof Expressions.Variable) {
            String name = ((Expressions.Variable) expr).name;
            if (params != null) {
                if (name.startsWith("?")) {
                    try {
                        int idx = name.length() > 1 ? Integer.parseInt(name.substring(1)) - 1 : 0;
                        if (idx >= 0 && idx < params.length && params[idx] != null) return params[idx];
                    } catch (NumberFormatException e) {
                        // fall through
                    }
                }
                // Named parameters ($name, :name, @name)
                for (SQLiteValue p : params) {
                    return p; // simplified
                }
            }
            return SQLiteValue.fromNull();
        }

        if (expr instanceof Expressions.ColumnRef) {
            Expressions.ColumnRef cr = (Expressions.ColumnRef) expr;
            if (row == null) return SQLiteValue.fromNull();

            // Check for rowid
            if ("rowid".equalsIgnoreCase(cr.columnName) || "_rowid_".equalsIgnoreCase(cr.columnName) || "oid".equalsIgnoreCase(cr.columnName)) {
                return SQLiteValue.fromLong(row.rowid);
            }

            // If table prefix specified, search only columns belonging to that table
            if (cr.tableName != null && row.tableNames != null && !row.tableNames.isEmpty()) {
                for (int i = 0; i < row.columnNames.size(); i++) {
                    if (i < row.tableNames.size() && row.tableNames.get(i).equalsIgnoreCase(cr.tableName)) {
                        if (row.columnNames.get(i).equalsIgnoreCase(cr.columnName)) {
                            return i < row.values.length ? row.values[i] : SQLiteValue.fromNull();
                        }
                    }
                }
                return SQLiteValue.fromNull();
            }

            // No table prefix - find column by name
            for (int i = 0; i < row.columnNames.size(); i++) {
                if (row.columnNames.get(i).equalsIgnoreCase(cr.columnName)) {
                    return i < row.values.length ? row.values[i] : SQLiteValue.fromNull();
                }
            }

            // Try with table prefix against row.tableName (legacy fallback)
            if (cr.tableName != null) {
                if (row.tableName != null && row.tableName.equalsIgnoreCase(cr.tableName)) {
                    for (int i = 0; i < row.columnNames.size(); i++) {
                        if (row.columnNames.get(i).equalsIgnoreCase(cr.columnName)) {
                            return i < row.values.length ? row.values[i] : SQLiteValue.fromNull();
                        }
                    }
                }
            }

            return SQLiteValue.fromNull();
        }

        if (expr instanceof Expressions.RowIdRef) {
            if (row == null) return SQLiteValue.fromNull();
            return SQLiteValue.fromLong(row.rowid);
        }

        if (expr instanceof Expressions.Binary) {
            Expressions.Binary bin = (Expressions.Binary) expr;
            SQLiteValue left = evaluateExpression(bin.left, row, params);
            SQLiteValue right = evaluateExpression(bin.right, row, params);

            switch (bin.operator) {
                case ADD: return SQLiteValue.fromDouble(left.asDouble() + right.asDouble());
                case SUBTRACT: return SQLiteValue.fromDouble(left.asDouble() - right.asDouble());
                case MULTIPLY: return SQLiteValue.fromDouble(left.asDouble() * right.asDouble());
                case DIVIDE:
                    double divisor = right.asDouble();
                    return divisor == 0 ? SQLiteValue.fromNull() : SQLiteValue.fromDouble(left.asDouble() / divisor);
                case MODULO:
                    double mod = right.asDouble();
                    return mod == 0 ? SQLiteValue.fromNull() : SQLiteValue.fromDouble(left.asDouble() % mod);
                case CONCAT:
                    String ls = left.asString(), rs = right.asString();
                    return (ls == null || rs == null) ? SQLiteValue.fromNull() : SQLiteValue.fromText(ls + rs);
                case BIT_AND: return SQLiteValue.fromLong(left.asLong() & right.asLong());
                case BIT_OR: return SQLiteValue.fromLong(left.asLong() | right.asLong());
                case LSHIFT: return SQLiteValue.fromLong(left.asLong() << right.asLong());
                case RSHIFT: return SQLiteValue.fromLong(left.asLong() >> right.asLong());
                case EQ: return boolVal(left.compareTo(right) == 0);
                case NEQ: return boolVal(left.compareTo(right) != 0);
                case LT: return boolVal(left.compareTo(right) < 0);
                case LTE: return boolVal(left.compareTo(right) <= 0);
                case GT: return boolVal(left.compareTo(right) > 0);
                case GTE: return boolVal(left.compareTo(right) >= 0);
                case AND: return boolVal(left.asLong() != 0 && right.asLong() != 0);
                case OR: return boolVal(left.asLong() != 0 || right.asLong() != 0);
                case IS: return boolVal(left.isNull() && right.isNull() || (!left.isNull() && !right.isNull() && left.compareTo(right) == 0));
                case IS_NOT: return boolVal(!left.isNull() || !right.isNull() || left.compareTo(right) != 0);
                case LIKE: return boolVal(likeMatch(left.asString(), right.asString()));
                case GLOB: return boolVal(globMatch(left.asString(), right.asString()));
                default: return SQLiteValue.fromNull();
            }
        }

        if (expr instanceof Expressions.Unary) {
            Expressions.Unary un = (Expressions.Unary) expr;
            SQLiteValue operand = evaluateExpression(un.operand, row, params);
            switch (un.operator) {
                case NEGATE:
                    if (operand.isInteger()) return SQLiteValue.fromLong(-operand.asLong());
                    return SQLiteValue.fromDouble(-operand.asDouble());
                case NOT: return boolVal(operand.asLong() == 0);
                case BIT_NOT: return SQLiteValue.fromLong(~operand.asLong());
                default: return SQLiteValue.fromNull();
            }
        }

        if (expr instanceof Expressions.FunctionCall) {
            Expressions.FunctionCall fc = (Expressions.FunctionCall) expr;
            String fnName = fc.name;

            SQLiteValue[] args = new SQLiteValue[fc.arguments != null ? fc.arguments.size() : 0];
            for (int i = 0; i < args.length; i++) {
                args[i] = evaluateExpression(fc.arguments.get(i), row, params);
            }

            // Handle COUNT(*)
            if ("count".equalsIgnoreCase(fnName) && fc.star) {
                if (row != null && row.groupRows != null) {
                    return SQLiteValue.fromLong(row.groupRows.size());
                }
                return SQLiteValue.fromLong(1);
            }

            // Handle aggregate functions on grouped rows
            if (row != null && row.groupRows != null && isAggregateFunction(fnName)) {
                return evaluateAggregate(fnName, row.groupRows, fc, params);
            }

            // Scalar function
            FunctionRegistry fn = db.getFunctionRegistry();
            if (fn.hasFunction(fnName)) {
                return fn.call(fnName, args);
            }

            return SQLiteValue.fromNull();
        }

        if (expr instanceof Expressions.Case) {
            Expressions.Case caseExpr = (Expressions.Case) expr;
            SQLiteValue operand = caseExpr.operand != null ? evaluateExpression(caseExpr.operand, row, params) : null;

            if (caseExpr.whenClauses != null) {
                for (Expressions.WhenThen wt : caseExpr.whenClauses) {
                    SQLiteValue whenVal = evaluateExpression(wt.when, row, params);
                    if (operand != null) {
                        if (operand.compareTo(whenVal) == 0) return evaluateExpression(wt.then, row, params);
                    } else {
                        if (whenVal != null && !whenVal.isNull() && whenVal.asLong() != 0) return evaluateExpression(wt.then, row, params);
                    }
                }
            }
            return caseExpr.elseExpression != null ? evaluateExpression(caseExpr.elseExpression, row, params) : SQLiteValue.fromNull();
        }

        if (expr instanceof Expressions.Cast) {
            Expressions.Cast cast = (Expressions.Cast) expr;
            SQLiteValue val = evaluateExpression(cast.expression, row, params);
            if (cast.typeName == null) return val;
            String type = cast.typeName.name.toUpperCase();
            switch (type) {
                case "INTEGER": return SQLiteValue.fromLong(val.asLong());
                case "REAL": case "FLOAT": case "DOUBLE": return SQLiteValue.fromDouble(val.asDouble());
                case "TEXT": case "VARCHAR": case "CHAR": return SQLiteValue.fromText(val.asString() != null ? val.asString() : "");
                case "BLOB": return val.isBlob() ? val : SQLiteValue.fromBlob(val.asString() != null ? val.asString().getBytes() : new byte[0]);
                case "NUMERIC":
                    try { return SQLiteValue.fromLong(val.asLong()); }
                    catch (Exception e) { return SQLiteValue.fromDouble(val.asDouble()); }
                default: return val;
            }
        }

        if (expr instanceof Expressions.Between) {
            Expressions.Between bt = (Expressions.Between) expr;
            SQLiteValue val = evaluateExpression(bt.expression, row, params);
            SQLiteValue low = evaluateExpression(bt.low, row, params);
            SQLiteValue high = evaluateExpression(bt.high, row, params);
            boolean result = val.compareTo(low) >= 0 && val.compareTo(high) <= 0;
            return boolVal(bt.not ? !result : result);
        }

        if (expr instanceof Expressions.In) {
            Expressions.In in = (Expressions.In) expr;
            SQLiteValue val = evaluateExpression(in.expression, row, params);
            if (in.values != null) {
                for (Expression e : in.values) {
                    SQLiteValue v = evaluateExpression(e, row, params);
                    if (val.compareTo(v) == 0) return boolVal(!in.not);
                }
                return boolVal(in.not);
            }
            if (in.subquery != null) {
                try {
                    Result subResult = executeSelect(in.subquery, params);
                    for (int i = 0; i < subResult.getRowCount(); i++) {
                        SQLiteValue v = subResult.getValue(i, 0);
                        if (val.compareTo(v) == 0) return boolVal(!in.not);
                    }
                } catch (Exception e) {
                    return SQLiteValue.fromNull();
                }
                return boolVal(in.not);
            }
            return boolVal(in.not);
        }

        if (expr instanceof Expressions.Exists) {
            Expressions.Exists ex = (Expressions.Exists) expr;
            try {
                Result subResult = executeSelect(ex.subquery, params);
                return boolVal(subResult.getRowCount() > 0 != ex.not);
            } catch (Exception e) {
                return boolVal(false);
            }
        }

        if (expr instanceof Expressions.Subquery) {
            try {
                Result subResult = executeSelect(((Expressions.Subquery) expr).subquery, params);
                if (subResult.getRowCount() > 0 && subResult.getColumnCount() > 0) {
                    return subResult.getValue(0, 0);
                }
            } catch (Exception e) {
                // fall through
            }
            return SQLiteValue.fromNull();
        }

        if (expr instanceof Expressions.IsNull) {
            Expressions.IsNull isn = (Expressions.IsNull) expr;
            SQLiteValue val = evaluateExpression(isn.expression, row, params);
            return boolVal(isn.isNotNull ? !val.isNull() : val.isNull());
        }

        if (expr instanceof Expressions.Collate) {
            return evaluateExpression(((Expressions.Collate) expr).expression, row, params);
        }

        if (expr instanceof Expressions.Raise) {
            Expressions.Raise raise = (Expressions.Raise) expr;
            if (raise.type == Enums.RaiseType.IGNORE) return SQLiteValue.fromNull();
            throw new RuntimeException(raise.message != null ? raise.message : "RAISE");
        }

        return SQLiteValue.fromNull();
    }

    private boolean isAggregateFunction(String name) {
        String lower = name.toLowerCase();
        return lower.equals("count") || lower.equals("sum") || lower.equals("total") ||
               lower.equals("avg") || lower.equals("min") || lower.equals("max") ||
               lower.equals("group_concat");
    }

    private SQLiteValue evaluateAggregate(String fnName, List<TableRow> group, Expressions.FunctionCall fc, SQLiteValue[] params) {
        String name = fnName.toLowerCase();
        switch (name) {
            case "count":
                return SQLiteValue.fromLong(group.size());
            case "sum": {
                double sum = 0;
                for (TableRow tr : group) {
                    SQLiteValue val = evaluateExpression(fc.arguments.get(0), tr, params);
                    if (!val.isNull()) sum += val.asDouble();
                }
                return SQLiteValue.fromDouble(sum);
            }
            case "total": {
                double sum = 0;
                for (TableRow tr : group) {
                    SQLiteValue val = evaluateExpression(fc.arguments.get(0), tr, params);
                    if (!val.isNull()) sum += val.asDouble();
                }
                return SQLiteValue.fromDouble(sum);
            }
            case "avg": {
                double sum = 0;
                int count = 0;
                for (TableRow tr : group) {
                    SQLiteValue val = evaluateExpression(fc.arguments.get(0), tr, params);
                    if (!val.isNull()) { sum += val.asDouble(); count++; }
                }
                return count > 0 ? SQLiteValue.fromDouble(sum / count) : SQLiteValue.fromNull();
            }
            case "min": {
                SQLiteValue min = null;
                for (TableRow tr : group) {
                    SQLiteValue val = evaluateExpression(fc.arguments.get(0), tr, params);
                    if (!val.isNull() && (min == null || val.compareTo(min) < 0)) min = val;
                }
                return min != null ? min : SQLiteValue.fromNull();
            }
            case "max": {
                SQLiteValue max = null;
                for (TableRow tr : group) {
                    SQLiteValue val = evaluateExpression(fc.arguments.get(0), tr, params);
                    if (!val.isNull() && (max == null || val.compareTo(max) > 0)) max = val;
                }
                return max != null ? max : SQLiteValue.fromNull();
            }
            case "group_concat": {
                StringBuilder sb = new StringBuilder();
                String sep = ",";
                if (fc.arguments != null && fc.arguments.size() > 1) {
                    SQLiteValue sepVal = evaluateExpression(fc.arguments.get(1), null, params);
                    if (sepVal != null && !sepVal.isNull()) sep = sepVal.asString();
                }
                boolean first = true;
                for (TableRow tr : group) {
                    SQLiteValue val = evaluateExpression(fc.arguments.get(0), tr, params);
                    if (!val.isNull()) {
                        if (!first) sb.append(sep);
                        sb.append(val.asString());
                        first = false;
                    }
                }
                return sb.length() > 0 ? SQLiteValue.fromText(sb.toString()) : SQLiteValue.fromNull();
            }
            default:
                return SQLiteValue.fromNull();
        }
    }

    private SQLiteValue boolVal(boolean b) { return SQLiteValue.fromLong(b ? 1 : 0); }

    private boolean likeMatch(String text, String pattern) {
        if (text == null || pattern == null) return false;
        String regex = "^" + pattern.replace("%", ".*").replace("_", ".") + "$";
        return text.toUpperCase().matches(regex.toUpperCase());
    }

    private boolean globMatch(String text, String pattern) {
        if (text == null || pattern == null) return false;
        String regex = "^" + pattern.replace("*", ".*").replace("?", ".") + "$";
        return text.matches(regex);
    }

    private byte[] parseHexBlob(String hex) {
        if (hex == null) return new byte[0];
        hex = hex.trim();
        byte[] result = new byte[hex.length() / 2];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return result;
    }

    // ==================== INSERT ====================

    private Result executeInsert(InsertStatement ins, SQLiteValue[] params) throws Exception {
        TableInfo table = db.getTable(ins.tableName);
        if (table == null) throw new Exception("no such table: " + ins.tableName);

        if (ins.select != null) {
            // INSERT ... SELECT
            Result selResult = executeSelect(ins.select, params);
            int count = 0;
            for (int i = 0; i < selResult.getRowCount(); i++) {
                List<SQLiteValue> vals = new ArrayList<>();
                for (int j = 0; j < selResult.getColumnCount(); j++) {
                    vals.add(selResult.getValue(i, j));
                }
                db.insertRow(ins.tableName, ins.columns, vals);
                count++;
            }
            return Result.insert(count, db.getLastInsertRowid());
        }

        if (ins.values != null && !ins.values.isEmpty()) {
            int count = 0;
            for (List<Expression> rowExprs : ins.values) {
                List<SQLiteValue> vals = new ArrayList<>();
                for (Expression e : rowExprs) {
                    vals.add(evaluateExpression(e, null, params));
                }
                db.insertRow(ins.tableName, ins.columns, vals);
                count++;
            }
            return Result.insert(count, db.getLastInsertRowid());
        }

        return Result.insert(0, 0);
    }

    // ==================== UPDATE ====================

    private Result executeUpdate(UpdateStatement upd, SQLiteValue[] params) throws Exception {
        TableInfo table = db.getTable(upd.tableName);
        if (table == null) throw new Exception("no such table: " + upd.tableName);

        List<String> columns = new ArrayList<>();
        List<SQLiteValue> values = new ArrayList<>();
        for (ColumnAssignment ca : upd.assignments) {
            columns.add(ca.columnName);
            values.add(evaluateExpression(ca.value, null, params));
        }

        // For now, update all rows (WHERE support is simplified)
        int count = db.updateRows(upd.tableName, columns, values, null);
        return Result.update(count);
    }

    // ==================== DELETE ====================

    private Result executeDelete(DeleteStatement del, SQLiteValue[] params) throws Exception {
        TableInfo table = db.getTable(del.tableName);
        if (table == null) throw new Exception("no such table: " + del.tableName);

        // For now, delete all rows (WHERE support is simplified)
        int count = db.deleteRows(del.tableName, null);
        return Result.update(count);
    }

    // ==================== DDL ====================

    private Result executeCreateTable(CreateTableStatement stmt) throws Exception {
        if (db.getTable(stmt.tableName) != null && !stmt.ifNotExists) {
            throw new Exception("table " + stmt.tableName + " already exists");
        }
        if (db.getTable(stmt.tableName) != null) return Result.EMPTY;
        db.createTable(stmt);
        return Result.EMPTY;
    }

    private Result executeCreateIndex(CreateIndexStatement stmt) throws Exception {
        db.createIndex(stmt);
        return Result.EMPTY;
    }

    private Result executeCreateView(CreateViewStatement stmt) throws Exception {
        // Store view definition in sqlite_master
        try {
            BTree btree = db.getBTree();
            Record rec = new Record();
            rec.addValue(SQLiteValue.fromText("view"));
            rec.addValue(SQLiteValue.fromText(stmt.viewName));
            rec.addValue(SQLiteValue.fromText(stmt.viewName));
            rec.addValue(SQLiteValue.fromLong(0)); // views have no root page
            rec.addValue(SQLiteValue.fromText(reconstructSQL(stmt)));
            long rowid = btree.nextRowid(1);
            btree.insert(1, rowid, rec);
            db.getPager().setSchemaCookie(db.getPager().getSchemaCookie() + 1);
            db.getPager().commit();
        } catch (IOException e) {
            throw new Exception("Failed to create view: " + e.getMessage(), e);
        }
        return Result.EMPTY;
    }

    private Result executeCreateTrigger(CreateTriggerStatement stmt) throws Exception {
        try {
            BTree btree = db.getBTree();
            Record rec = new Record();
            rec.addValue(SQLiteValue.fromText("trigger"));
            rec.addValue(SQLiteValue.fromText(stmt.triggerName));
            rec.addValue(SQLiteValue.fromText(stmt.tableName));
            rec.addValue(SQLiteValue.fromLong(0));
            rec.addValue(SQLiteValue.fromText(reconstructSQL(stmt)));
            long rowid = btree.nextRowid(1);
            btree.insert(1, rowid, rec);
            db.getPager().setSchemaCookie(db.getPager().getSchemaCookie() + 1);
            db.getPager().commit();
        } catch (IOException e) {
            throw new Exception("Failed to create trigger: " + e.getMessage(), e);
        }
        return Result.EMPTY;
    }

    private Result executeAlterTable(AlterTableStatement stmt) throws Exception {
        switch (stmt.alterType) {
            case RENAME_TO:
                db.alterTableRenameTo(stmt.tableName, stmt.newTableName);
                break;
            case RENAME_COLUMN:
                db.alterTableRenameColumn(stmt.tableName, stmt.renameColumnOld, stmt.renameColumnNew);
                break;
            case ADD_COLUMN:
                db.alterTableAddColumn(stmt.tableName, stmt.addColumn);
                break;
            case DROP_COLUMN:
                db.alterTableDropColumn(stmt.tableName, stmt.dropColumnName);
                break;
            default:
                throw new Exception("Unsupported ALTER TABLE operation");
        }
        return Result.EMPTY;
    }

    private Result executeDropTable(DropTable stmt) throws Exception {
        if (db.getTable(stmt.tableName) == null && !stmt.ifExists) {
            throw new Exception("no such table: " + stmt.tableName);
        }
        if (db.getTable(stmt.tableName) != null) {
            db.dropTable(stmt.tableName);
        }
        return Result.EMPTY;
    }

    private Result executeDropIndex(DropIndex stmt) throws Exception {
        if (db.getIndex(stmt.indexName) == null && !stmt.ifExists) {
            throw new Exception("no such index: " + stmt.indexName);
        }
        if (db.getIndex(stmt.indexName) != null) {
            db.dropIndex(stmt.indexName);
        }
        return Result.EMPTY;
    }

    private Result executeDropView(DropView stmt) throws Exception {
        // Remove from sqlite_master
        return Result.EMPTY;
    }

    private Result executeDropTrigger(DropTrigger stmt) throws Exception {
        return Result.EMPTY;
    }

    // ==================== Transactions ====================

    private Result executeBegin(Begin stmt) throws Exception {
        db.beginTransaction();
        return Result.EMPTY;
    }

    private Result executeCommit() throws Exception {
        db.commitTransaction();
        return Result.EMPTY;
    }

    private Result executeRollback() throws Exception {
        db.rollbackTransaction();
        return Result.EMPTY;
    }

    private Result executeSavepoint(Savepoint stmt) throws Exception {
        // Simplified savepoint support
        return Result.EMPTY;
    }

    // ==================== PRAGMA ====================

    private Result executePragma(PragmaStatement stmt) throws Exception {
        String name = stmt.pragmaName.toLowerCase();

        if (stmt.value == null) {
            // Read pragma
            switch (name) {
                case "table_info": {
                    // PRAGMA table_info(table_name) - special handling
                    return Result.EMPTY;
                }
                case "database_list": {
                    String[] cols = {"seq", "name", "file"};
                    List<SQLiteValue[]> rows = new ArrayList<>();
                    rows.add(new SQLiteValue[]{SQLiteValue.fromLong(0), SQLiteValue.fromText("main"), SQLiteValue.fromText("")});
                    return Result.query(cols, rows);
                }
                case "version": {
                    String[] cols = {"version"};
                    List<SQLiteValue[]> rows = new ArrayList<>();
                    rows.add(new SQLiteValue[]{SQLiteValue.fromText("1.0.0")});
                    return Result.query(cols, rows);
                }
                case "schema_version": {
                    String[] cols = {"schema_version"};
                    List<SQLiteValue[]> rows = new ArrayList<>();
                    rows.add(new SQLiteValue[]{SQLiteValue.fromLong(db.getPager().getSchemaCookie())});
                    return Result.query(cols, rows);
                }
                case "user_version": {
                    String[] cols = {"user_version"};
                    List<SQLiteValue[]> rows = new ArrayList<>();
                    rows.add(new SQLiteValue[]{SQLiteValue.fromLong(db.getPager().getUserVersion())});
                    return Result.query(cols, rows);
                }
                case "page_size": {
                    String[] cols = {"page_size"};
                    List<SQLiteValue[]> rows = new ArrayList<>();
                    rows.add(new SQLiteValue[]{SQLiteValue.fromLong(db.getPager().getPageSize())});
                    return Result.query(cols, rows);
                }
                case "page_count": {
                    String[] cols = {"page_count"};
                    List<SQLiteValue[]> rows = new ArrayList<>();
                    rows.add(new SQLiteValue[]{SQLiteValue.fromLong(db.getPager().getTotalPages())});
                    return Result.query(cols, rows);
                }
                case "journal_mode": {
                    String[] cols = {"journal_mode"};
                    List<SQLiteValue[]> rows = new ArrayList<>();
                    rows.add(new SQLiteValue[]{SQLiteValue.fromText(db.getPager().isWalMode() ? "wal" : "delete")});
                    return Result.query(cols, rows);
                }
                default:
                    return Result.EMPTY;
            }
        } else {
            // Set pragma
            SQLiteValue val = evaluateExpression(stmt.value, null, null);
            switch (name) {
                case "user_version":
                    db.getPager().setUserVersion((int) val.asLong());
                    break;
                case "application_id":
                    db.getPager().setApplicationId((int) val.asLong());
                    break;
                case "journal_mode":
                    String mode = val.asString();
                    // WAL mode switching would require more work
                    break;
            }
            return Result.EMPTY;
        }
    }

    private Result executeVacuum() {
        // VACUUM would rebuild the database file
        return Result.EMPTY;
    }

    private Result executeReindex(ReindexStatement stmt) {
        return Result.EMPTY;
    }

    private Result executeAttach(AttachStatement stmt) {
        return Result.EMPTY;
    }

    private Result executeDetach(DetachStatement stmt) {
        return Result.EMPTY;
    }

    private Result executeExplain(ExplainStatement stmt) throws Exception {
        if (stmt.statement == null) return Result.EMPTY;

        if (stmt.queryPlan) {
            // EXPLAIN QUERY PLAN: return a high-level query plan
            return buildQueryPlan(stmt.statement);
        } else {
            // EXPLAIN: return VDBE-style opcodes
            String[] cols = {"addr", "opcode", "p1", "p2", "p3", "p4", "comment"};
            List<SQLiteValue[]> rows = new ArrayList<>();
            rows.add(new SQLiteValue[]{SQLiteValue.fromLong(0), SQLiteValue.fromText("Trace"),
                SQLiteValue.fromLong(0), SQLiteValue.fromLong(0), SQLiteValue.fromLong(0),
                SQLiteValue.fromNull(), SQLiteValue.fromText(stmt.statement.getClass().getSimpleName())});
            return Result.query(cols, rows);
        }
    }

    private Result buildQueryPlan(Statement stmt) {
        String[] cols = {"selectid", "order", "from", "detail"};
        List<SQLiteValue[]> rows = new ArrayList<>();
        int[] order = {0};

        if (stmt instanceof SelectStatement) {
            buildSelectPlan((SelectStatement) stmt, rows, order);
        } else if (stmt instanceof InsertStatement) {
            InsertStatement ins = (InsertStatement) stmt;
            TableInfo table = db.getTable(ins.tableName);
            rows.add(planRow(0, order[0]++, 0, "INSERT INTO " + ins.tableName + " (rowid=" + (table != null ? table.rootPage : "?") + ")"));
        } else if (stmt instanceof UpdateStatement) {
            UpdateStatement upd = (UpdateStatement) stmt;
            TableInfo table = db.getTable(upd.tableName);
            rows.add(planRow(0, order[0]++, 0, "UPDATE " + upd.tableName + " (rowid=" + (table != null ? table.rootPage : "?") + ")"));
        } else if (stmt instanceof DeleteStatement) {
            DeleteStatement del = (DeleteStatement) stmt;
            TableInfo table = db.getTable(del.tableName);
            rows.add(planRow(0, order[0]++, 0, "DELETE FROM " + del.tableName + " (rowid=" + (table != null ? table.rootPage : "?") + ")"));
        } else {
            rows.add(planRow(0, 0, 0, stmt.getClass().getSimpleName()));
        }

        return Result.query(cols, rows);
    }

    private void buildSelectPlan(SelectStatement sel, List<SQLiteValue[]> rows, int[] order) {
        if (sel.from == null || sel.from.isEmpty()) {
            rows.add(planRow(0, order[0]++, 0, "SCAN CONSTANT ROW"));
            return;
        }

        Map<String, List<Expression>> constraints = new LinkedHashMap<>();
        if (sel.where != null) {
            extractColumnConstraints(sel.where, constraints);
        }

        int fromIdx = 0;
        for (TableOrSubquery tos : sel.from) {
            if (tos.type == TableOrSubquery.Type.JOIN) {
                collectJoinPlanTables(tos, rows, order, 0, sel.where);
            } else if (tos.type == TableOrSubquery.Type.TABLE) {
                String tblName = tos.alias != null ? tos.alias : tos.tableName;
                String detail = buildScanOrSearchDetail(tos, tblName, sel.where, constraints);
                rows.add(planRow(0, order[0]++, fromIdx, detail));
                fromIdx++;
            } else if (tos.type == TableOrSubquery.Type.SUBQUERY) {
                String alias = tos.alias != null ? tos.alias : "subquery";
                rows.add(planRow(0, order[0]++, fromIdx, "SCAN SUBQUERY " + alias));
                fromIdx++;
            }
        }

        if (sel.where != null) {
            rows.add(planRow(0, order[0]++, 0, "FILTER " + exprToString(sel.where)));
        }

        if (sel.groupBy != null && !sel.groupBy.isEmpty()) {
            rows.add(planRow(0, order[0]++, 0, "USE GROUP BY"));
        }

        if (sel.orderBy != null && !sel.orderBy.isEmpty()) {
            rows.add(planRow(0, order[0]++, 0, "USE ORDER BY"));
        }

        if (sel.distinct) {
            rows.add(planRow(0, order[0]++, 0, "USE DISTINCT"));
        }

        if (sel.limit != null) {
            rows.add(planRow(0, order[0]++, 0, "USE LIMIT"));
        }
    }

    private void collectJoinPlanTables(TableOrSubquery join, List<SQLiteValue[]> rows, int[] order, int depth, Expression whereExpr) {
        if (join.left != null) {
            if (join.left.type == TableOrSubquery.Type.JOIN) {
                collectJoinPlanTables(join.left, rows, order, depth + 1, whereExpr);
            } else if (join.left.type == TableOrSubquery.Type.TABLE) {
                String tblName = join.left.alias != null ? join.left.alias : join.left.tableName;
                Map<String, List<Expression>> constraints = new LinkedHashMap<>();
                if (whereExpr != null) extractColumnConstraints(whereExpr, constraints);
                if (join.onCondition != null) extractColumnConstraints(join.onCondition, constraints);
                String detail = buildScanOrSearchDetail(join.left, tblName, whereExpr, constraints);
                rows.add(planRow(0, order[0]++, 0, detail));
            }
        }

        if (join.right != null) {
            Map<String, List<Expression>> constraints = new LinkedHashMap<>();
            if (join.onCondition != null) extractColumnConstraints(join.onCondition, constraints);
            String tblName = join.right.alias != null ? join.right.alias : join.right.tableName;
            String detail = buildScanOrSearchDetail(join.right, tblName, join.onCondition, constraints);
            String joinType = join.joinType != null ? join.joinType.name() : "INNER";

            if (join.onCondition != null) {
                detail = joinType + " JOIN " + detail + " ON " + exprToString(join.onCondition);
            } else {
                detail = joinType + " JOIN " + detail;
            }
            rows.add(planRow(0, order[0]++, 1, detail));
        }
    }

    private void extractColumnConstraints(Expression expr, Map<String, List<Expression>> constraints) {
        if (expr instanceof Expressions.Binary) {
            Expressions.Binary bin = (Expressions.Binary) expr;
            if (bin.operator == BinaryOp.AND) {
                extractColumnConstraints(bin.left, constraints);
                extractColumnConstraints(bin.right, constraints);
                return;
            }
            if (bin.operator == BinaryOp.EQ || bin.operator == BinaryOp.LT || bin.operator == BinaryOp.LTE
                || bin.operator == BinaryOp.GT || bin.operator == BinaryOp.GTE
                || bin.operator == BinaryOp.IS || bin.operator == BinaryOp.IS_NOT) {
                String colName = extractColumnName(bin.left);
                if (colName == null) colName = extractColumnName(bin.right);
                if (colName != null) {
                    constraints.computeIfAbsent(colName.toLowerCase(), k -> new ArrayList<>()).add(expr);
                }
            }
        } else if (expr instanceof Expressions.In) {
            Expressions.In inExpr = (Expressions.In) expr;
            String colName = extractColumnName(inExpr.expression);
            if (colName != null) {
                constraints.computeIfAbsent(colName.toLowerCase(), k -> new ArrayList<>()).add(expr);
            }
        } else if (expr instanceof Expressions.Between) {
            Expressions.Between btw = (Expressions.Between) expr;
            String colName = extractColumnName(btw.expression);
            if (colName != null) {
                constraints.computeIfAbsent(colName.toLowerCase(), k -> new ArrayList<>()).add(expr);
            }
        } else if (expr instanceof Expressions.IsNull) {
            Expressions.IsNull isNull = (Expressions.IsNull) expr;
            String colName = extractColumnName(isNull.expression);
            if (colName != null) {
                constraints.computeIfAbsent(colName.toLowerCase(), k -> new ArrayList<>()).add(expr);
            }
        }
    }

    private String extractColumnName(Expression expr) {
        if (expr instanceof Expressions.ColumnRef) {
            return ((Expressions.ColumnRef) expr).columnName;
        }
        if (expr instanceof Expressions.RowIdRef) {
            return "rowid";
        }
        return null;
    }

    private IndexInfo findMatchingIndex(String tableName, Map<String, List<Expression>> constraints) {
        if (constraints == null || constraints.isEmpty()) return null;
        TableInfo table = db.getTable(tableName);
        if (table == null) return null;

        IndexInfo best = null;
        int bestPrefixLen = 0;

        for (IndexInfo idx : db.getIndexes()) {
            if (!idx.tableName.equalsIgnoreCase(tableName)) continue;
            if (idx.columns == null || idx.columns.isEmpty()) continue;

            int prefixLen = 0;
            for (String idxCol : idx.columns) {
                if (constraints.containsKey(idxCol.toLowerCase())) {
                    prefixLen++;
                } else {
                    break;
                }
            }
            if (prefixLen > bestPrefixLen) {
                bestPrefixLen = prefixLen;
                best = idx;
            }
        }

        if (bestPrefixLen == 0) return null;
        return best;
    }

    private String buildScanOrSearchDetail(TableOrSubquery tos, String displayTableName,
                                            Expression whereExpr, Map<String, List<Expression>> constraints) {
        String tblName = tos.tableName;
        String alias = tos.alias;
        TableInfo table = db.getTable(tblName);

        IndexInfo matchedIndex = null;
        if (tos.notIndexed) {
            // NOT INDEXED forced
        } else if (tos.indexed && tos.indexName != null) {
            IndexInfo forced = db.getIndex(tos.indexName);
            if (forced != null) matchedIndex = forced;
        } else {
            matchedIndex = findMatchingIndex(tblName, constraints);
        }

        StringBuilder sb = new StringBuilder();
        if (matchedIndex != null) {
            sb.append("SEARCH TABLE ").append(tblName);
            if (alias != null && !alias.equalsIgnoreCase(tblName)) {
                sb.append(" AS ").append(alias);
            }
            sb.append(" USING INDEX ").append(matchedIndex.name);
            sb.append(" (");
            for (int i = 0; i < matchedIndex.columns.size(); i++) {
                String col = matchedIndex.columns.get(i);
                if (i > 0) sb.append(" AND ");
                if (constraints.containsKey(col.toLowerCase())) {
                    sb.append(col).append("=?");
                } else {
                    sb.append(col).append(">?");
                }
            }
            sb.append(")");
            if (table != null) {
                sb.append(" (~").append(estimateRowCount(table)).append(" rows)");
            }
        } else {
            sb.append("SCAN TABLE ").append(tblName);
            if (alias != null && !alias.equalsIgnoreCase(tblName)) {
                sb.append(" AS ").append(alias);
            }
            if (table != null) {
                sb.append(" (~").append(estimateRowCount(table)).append(" rows)");
            }
        }
        return sb.toString();
    }

    private SQLiteValue[] planRow(int selectId, int order, int from, String detail) {
        return new SQLiteValue[]{
            SQLiteValue.fromLong(selectId),
            SQLiteValue.fromLong(order),
            SQLiteValue.fromLong(from),
            SQLiteValue.fromText(detail)
        };
    }

    private long estimateRowCount(TableInfo table) {
        try {
            return db.getBTree().scanTable(table.rootPage).size();
        } catch (Exception e) {
            return 0;
        }
    }

    private String exprToString(Expression expr) {
        if (expr == null) return "";
        // Use the AST's toString for a readable representation
        String s = expr.toString();
        // Truncate very long expressions
        if (s.length() > 80) return s.substring(0, 77) + "...";
        return s;
    }

    // ==================== Helper Methods ====================

    private String reconstructSQL(CreateViewStatement stmt) {
        return "CREATE VIEW " + stmt.viewName + " AS " + (stmt.select != null ? "SELECT ..." : "");
    }

    private String reconstructSQL(CreateTriggerStatement stmt) {
        return "CREATE TRIGGER " + stmt.triggerName;
    }

    // ==================== Inner Classes ====================

    public static class TableRow {
        public String tableName;
        public List<String> columnNames;
        public List<String> tableNames; // per-column table alias/name
        public SQLiteValue[] values;
        public long rowid;
        public List<TableRow> groupRows;

        public TableRow() {
            this.columnNames = new ArrayList<>();
            this.tableNames = new ArrayList<>();
            this.values = new SQLiteValue[0];
        }
    }
}
