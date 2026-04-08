package com.jasqlite.sql.parser;

import com.jasqlite.sql.ast.*;
import com.jasqlite.sql.ast.Enums.*;
import com.jasqlite.sql.ast.Expressions.*;
import com.jasqlite.sql.ast.TableConstraint.*;
import com.jasqlite.sql.ast.TransactionStatements.*;
import com.jasqlite.sql.ast.DropStatements.*;
import com.jasqlite.sql.ast.Constraints.*;

import java.util.*;

public class SQLParser {
    private final List<Token> tokens;
    private int pos;
    private int paramCounter; // auto-incrementing counter for ? placeholders

    public SQLParser(List<Token> tokens) {
        this.tokens = tokens;
        this.pos = 0;
        this.paramCounter = 0;
    }

    public static SQLParser create(String sql) {
        Lexer lexer = new Lexer(sql);
        return new SQLParser(lexer.tokenize());
    }

    private Token peek() {
        if (pos < tokens.size()) return tokens.get(pos);
        return new Token(TokenType.EOF, "", -1);
    }

    private Token advance() {
        Token t = tokens.get(pos);
        pos++;
        return t;
    }

    private boolean check(TokenType type) { return peek().type == type; }

    private boolean match(TokenType... types) {
        for (TokenType type : types) {
            if (check(type)) { advance(); return true; }
        }
        return false;
    }

    private Token expect(TokenType type) throws ParseException {
        if (check(type)) return advance();
        Token t = peek();
        throw new ParseException("Expected " + type + " but found " + t.type + "('" + t.value + "')", t.position);
    }

    private boolean isAtEnd() { return peek().type == TokenType.EOF; }

    private String expectName() throws ParseException {
        Token t = peek();
        if (t.type == TokenType.IDENTIFIER || t.isKeyword()) return advance().value;
        throw new ParseException("Expected name but found " + t.type, t.position);
    }

    public Statement parse() throws ParseException {
        Statement stmt = parseStatement();
        return stmt;
    }

    public List<Statement> parseAll() throws ParseException {
        List<Statement> stmts = new ArrayList<>();
        while (!isAtEnd()) {
            while (match(TokenType.SEMICOLON)) {}
            if (isAtEnd()) break;
            stmts.add(parseStatement());
            while (match(TokenType.SEMICOLON)) {}
        }
        return stmts;
    }

    private Statement parseStatement() throws ParseException {
        Token t = peek();
        switch (t.type) {
            case SELECT: return parseSelect();
            case INSERT: case REPLACE: return parseInsert();
            case UPDATE: return parseUpdate();
            case DELETE: return parseDelete();
            case CREATE: return parseCreate();
            case ALTER: return parseAlterTable();
            case DROP: return parseDrop();
            case BEGIN: return parseBegin();
            case COMMIT: case END: advance(); return new Commit();
            case ROLLBACK: return parseRollback();
            case SAVEPOINT: return parseSavepoint();
            case RELEASE: return parseRelease();
            case PRAGMA: return parsePragma();
            case VACUUM: advance(); return new VacuumStatement();
            case REINDEX: return parseReindex();
            case ATTACH: return parseAttach();
            case DETACH: return parseDetach();
            case EXPLAIN: return parseExplain();
            case WITH: advance(); match(TokenType.RECURSIVE);
                while (!check(TokenType.SELECT)) advance();
                return parseSelect();
            default:
                throw new ParseException("Unexpected token: " + t.type + "('" + t.value + "')", t.position);
        }
    }

    // ==================== SELECT ====================
    private SelectStatement parseSelect() throws ParseException {
        SelectStatement sel = new SelectStatement();
        expect(TokenType.SELECT);
        if (match(TokenType.DISTINCT)) sel.distinct = true;
        else match(TokenType.ALL);
        sel.columns = parseResultColumns();
        if (match(TokenType.FROM)) sel.from = parseFromClause();
        sel.where = parseOptionalWhere();
        sel.groupBy = parseOptionalGroupBy();
        sel.having = parseOptionalHaving();
        if (check(TokenType.UNION) || check(TokenType.INTERSECT) || check(TokenType.EXCEPT)) {
            TokenType tt = advance().type;
            sel.unionType = tt == TokenType.UNION ? UnionType.UNION : tt == TokenType.INTERSECT ? UnionType.INTERSECT : UnionType.EXCEPT;
            if (tt == TokenType.UNION && match(TokenType.ALL)) sel.unionType = UnionType.UNION_ALL;
            sel.unions = new ArrayList<>();
            sel.unions.add(parseSelect());
        }
        sel.orderBy = parseOptionalOrderBy();
        sel.limit = parseOptionalLimit();
        if (sel.limit != null && match(TokenType.OFFSET)) sel.offset = parseExpression();
        return sel;
    }

    private List<ResultColumn> parseResultColumns() throws ParseException {
        List<ResultColumn> cols = new ArrayList<>();
        do { cols.add(parseResultColumn()); } while (match(TokenType.COMMA));
        return cols;
    }

    private ResultColumn parseResultColumn() throws ParseException {
        ResultColumn rc = new ResultColumn();
        if (check(TokenType.STAR)) { advance(); rc.isStar = true; return rc; }
        if (check(TokenType.IDENTIFIER)) {
            int save = pos;
            Token name = advance();
            if (check(TokenType.DOT)) { advance();
                if (check(TokenType.STAR)) { advance(); rc.starTableName = name.value; return rc; }
            }
            pos = save;
        }
        rc.expression = parseExpression();
        if (match(TokenType.AS)) rc.alias = expectName();
        else if (check(TokenType.IDENTIFIER) && !isReservedAfterAlias()) rc.alias = expectName();
        return rc;
    }

    private boolean isReservedAfterAlias() {
        TokenType t = peek().type;
        return t == TokenType.FROM || t == TokenType.WHERE || t == TokenType.COMMA || t == TokenType.SEMICOLON ||
               t == TokenType.RPAREN || t == TokenType.ORDER || t == TokenType.LIMIT || t == TokenType.EOF ||
               t == TokenType.UNION || t == TokenType.INTERSECT || t == TokenType.EXCEPT || t == TokenType.HAVING ||
               t == TokenType.GROUP || t == TokenType.ON || t == TokenType.JOIN || t == TokenType.INNER ||
               t == TokenType.LEFT || t == TokenType.RIGHT || t == TokenType.CROSS || t == TokenType.NATURAL;
    }

    private List<TableOrSubquery> parseFromClause() throws ParseException {
        List<TableOrSubquery> tables = new ArrayList<>();
        tables.add(parseTableOrSubquery());
        while (check(TokenType.COMMA) || check(TokenType.JOIN) || check(TokenType.INNER) ||
               check(TokenType.LEFT) || check(TokenType.RIGHT) || check(TokenType.FULL) || check(TokenType.CROSS) || check(TokenType.NATURAL)) {
            if (check(TokenType.COMMA)) { advance(); tables.add(parseTableOrSubquery()); continue; }
            TableOrSubquery join = new TableOrSubquery();
            join.type = TableOrSubquery.Type.JOIN;
            join.left = tables.remove(tables.size() - 1);
            boolean nat = match(TokenType.NATURAL);
            if (match(TokenType.LEFT)) { match(TokenType.OUTER); join.joinType = nat ? JoinType.NATURAL_LEFT : JoinType.LEFT; }
            else if (match(TokenType.RIGHT)) { match(TokenType.OUTER); join.joinType = nat ? JoinType.NATURAL_RIGHT : JoinType.RIGHT; }
            else if (match(TokenType.FULL)) { match(TokenType.OUTER); join.joinType = nat ? JoinType.NATURAL_FULL : JoinType.FULL; }
            else if (match(TokenType.CROSS)) { join.joinType = JoinType.CROSS; }
            else if (match(TokenType.INNER)) { join.joinType = nat ? JoinType.NATURAL_INNER : JoinType.INNER; }
            else { join.joinType = nat ? JoinType.NATURAL_INNER : JoinType.INNER; }
            expect(TokenType.JOIN);
            join.right = parseTableOrSubquery();
            if (match(TokenType.ON)) join.onCondition = parseExpression();
            else if (match(TokenType.USING)) {
                expect(TokenType.LPAREN);
                join.usingColumns = new ArrayList<>();
                do { join.usingColumns.add(expectName()); } while (match(TokenType.COMMA));
                expect(TokenType.RPAREN);
            }
            tables.add(join);
        }
        return tables;
    }

    private TableOrSubquery parseTableOrSubquery() throws ParseException {
        TableOrSubquery tos = new TableOrSubquery();
        if (check(TokenType.LPAREN)) {
            advance();
            tos.type = TableOrSubquery.Type.SUBQUERY;
            tos.subquery = parseSelect();
            expect(TokenType.RPAREN);
        } else {
            tos.type = TableOrSubquery.Type.TABLE;
            tos.tableName = expectName();
            if (match(TokenType.DOT)) { tos.databaseName = tos.tableName; tos.tableName = expectName(); }
        }
        if (match(TokenType.AS)) tos.alias = expectName();
        else if (check(TokenType.IDENTIFIER) && !isReservedAfterAlias()) tos.alias = expectName();
        return tos;
    }

    private Expression parseOptionalWhere() throws ParseException { if (match(TokenType.WHERE)) return parseExpression(); return null; }
    private List<Expression> parseOptionalGroupBy() throws ParseException {
        if (match(TokenType.GROUP)) { expect(TokenType.BY); List<Expression> l = new ArrayList<>(); do { l.add(parseExpression()); } while (match(TokenType.COMMA)); return l; }
        return null;
    }
    private Expression parseOptionalHaving() throws ParseException { if (match(TokenType.HAVING)) return parseExpression(); return null; }
    private List<OrderByItem> parseOptionalOrderBy() throws ParseException {
        if (match(TokenType.ORDER)) { expect(TokenType.BY); List<OrderByItem> l = new ArrayList<>(); do { l.add(parseOrderByItem()); } while (match(TokenType.COMMA)); return l; }
        return null;
    }
    private OrderByItem parseOrderByItem() throws ParseException {
        OrderByItem it = new OrderByItem(); it.expression = parseExpression();
        if (match(TokenType.ASC)) it.direction = OrderDirection.ASC; else if (match(TokenType.DESC)) it.direction = OrderDirection.DESC; else it.direction = OrderDirection.ASC;
        return it;
    }
    private Expression parseOptionalLimit() throws ParseException { if (match(TokenType.LIMIT)) return parseExpression(); return null; }

    // ==================== INSERT ====================
    private InsertStatement parseInsert() throws ParseException {
        InsertStatement ins = new InsertStatement();
        if (match(TokenType.REPLACE)) ins.isReplace = true;
        else { expect(TokenType.INSERT); if (match(TokenType.OR)) { if (match(TokenType.REPLACE)) ins.conflictAlgorithm = ConflictAlgorithm.REPLACE; else if (match(TokenType.IGNORE)) ins.conflictAlgorithm = ConflictAlgorithm.IGNORE; else expectName(); } }
        expect(TokenType.INTO); ins.tableName = expectName();
        if (match(TokenType.DOT)) { ins.databaseName = ins.tableName; ins.tableName = expectName(); }
        if (check(TokenType.LPAREN) && !checkNextSelect()) { advance(); if (!check(TokenType.RPAREN)) { ins.columns = new ArrayList<>(); do { ins.columns.add(expectName()); } while (match(TokenType.COMMA)); } expect(TokenType.RPAREN); }
        if (match(TokenType.VALUES)) { ins.values = new ArrayList<>(); do { expect(TokenType.LPAREN); List<Expression> row = new ArrayList<>(); if (!check(TokenType.RPAREN)) { do { row.add(parseExpression()); } while (match(TokenType.COMMA)); } expect(TokenType.RPAREN); ins.values.add(row); } while (match(TokenType.COMMA)); }
        else if (check(TokenType.SELECT)) ins.select = parseSelect();
        else if (match(TokenType.DEFAULT)) expect(TokenType.VALUES);
        if (match(TokenType.RETURNING)) ins.returning = parseExpression();
        return ins;
    }
    private boolean checkNextSelect() { int n = pos + 1; return n < tokens.size() && tokens.get(n).type == TokenType.SELECT; }

    // ==================== UPDATE ====================
    private UpdateStatement parseUpdate() throws ParseException {
        UpdateStatement upd = new UpdateStatement(); expect(TokenType.UPDATE);
        if (match(TokenType.OR)) { if (match(TokenType.REPLACE)) upd.conflictAlgorithm = ConflictAlgorithm.REPLACE; else if (match(TokenType.IGNORE)) upd.conflictAlgorithm = ConflictAlgorithm.IGNORE; else expectName(); }
        upd.tableName = expectName(); if (match(TokenType.DOT)) { upd.databaseName = upd.tableName; upd.tableName = expectName(); }
        expect(TokenType.SET); upd.assignments = new ArrayList<>();
        do { ColumnAssignment ca = new ColumnAssignment(); ca.columnName = expectName(); expect(TokenType.EQ); ca.value = parseExpression(); upd.assignments.add(ca); } while (match(TokenType.COMMA));
        upd.where = parseOptionalWhere(); upd.orderBy = parseOptionalOrderBy(); upd.limit = parseOptionalLimit();
        return upd;
    }

    // ==================== DELETE ====================
    private DeleteStatement parseDelete() throws ParseException {
        DeleteStatement del = new DeleteStatement(); expect(TokenType.DELETE); expect(TokenType.FROM);
        del.tableName = expectName(); if (match(TokenType.DOT)) { del.databaseName = del.tableName; del.tableName = expectName(); }
        del.where = parseOptionalWhere(); del.orderBy = parseOptionalOrderBy(); del.limit = parseOptionalLimit();
        return del;
    }

    // ==================== CREATE ====================
    private Statement parseCreate() throws ParseException {
        expect(TokenType.CREATE); boolean temp = match(TokenType.TEMP) || match(TokenType.TEMPORARY);
        if (check(TokenType.TABLE)) return parseCreateTable(temp);
        if (check(TokenType.VIRTUAL)) { advance(); expect(TokenType.TABLE); match(TokenType.IF); match(TokenType.NOT); match(TokenType.EXISTS); CreateTableStatement ct = new CreateTableStatement(); ct.tableName = expectName(); return ct; }
        if (check(TokenType.UNIQUE) || check(TokenType.INDEX)) return parseCreateIndex();
        if (check(TokenType.VIEW)) return parseCreateView(temp);
        if (check(TokenType.TRIGGER)) return parseCreateTrigger(temp);
        throw new ParseException("Expected TABLE, INDEX, VIEW, or TRIGGER after CREATE");
    }

    private CreateTableStatement parseCreateTable(boolean temp) throws ParseException {
        CreateTableStatement ct = new CreateTableStatement(); ct.temporary = temp; expect(TokenType.TABLE);
        if (match(TokenType.IF)) { expect(TokenType.NOT); expect(TokenType.EXISTS); ct.ifNotExists = true; }
        ct.tableName = expectName(); if (match(TokenType.DOT)) { ct.databaseName = ct.tableName; ct.tableName = expectName(); }
        if (match(TokenType.AS)) { ct.asSelect = parseSelect(); return ct; }
        expect(TokenType.LPAREN); ct.columns = new ArrayList<>(); ct.constraints = new ArrayList<>();
        do { if (check(TokenType.CONSTRAINT) || check(TokenType.PRIMARY) || check(TokenType.UNIQUE) || check(TokenType.CHECK) || check(TokenType.FOREIGN)) ct.constraints.add(parseTableConstraint()); else ct.columns.add(parseColumnDefinition()); } while (match(TokenType.COMMA));
        expect(TokenType.RPAREN); return ct;
    }

    private ColumnDefinition parseColumnDefinition() throws ParseException {
        ColumnDefinition col = new ColumnDefinition(); col.name = expectName();
        if (!check(TokenType.PRIMARY) && !check(TokenType.NOT) && !check(TokenType.NULL) && !check(TokenType.UNIQUE) && !check(TokenType.CHECK) && !check(TokenType.DEFAULT) && !check(TokenType.CONSTRAINT) && !check(TokenType.REFERENCES) && !check(TokenType.COLLATE) && !check(TokenType.GENERATED) && !check(TokenType.COMMA) && !check(TokenType.RPAREN))
            col.typeName = parseTypeName();
        col.constraints = new ArrayList<>();
        while (true) {
            if (match(TokenType.CONSTRAINT)) expectName();
            if (check(TokenType.PRIMARY)) { PrimaryKeyColumnConstraint pk = new PrimaryKeyColumnConstraint(); expect(TokenType.PRIMARY); expect(TokenType.KEY); if (match(TokenType.AUTOINCREMENT)) pk.autoIncrement = true; col.constraints.add(pk); }
            else if (check(TokenType.NOT)) { advance(); expect(TokenType.NULL); col.constraints.add(new NotNullConstraint()); }
            else if (match(TokenType.UNIQUE)) col.constraints.add(new UniqueColumnConstraint());
            else if (match(TokenType.DEFAULT)) { DefaultColumnConstraint dc = new DefaultColumnConstraint(); dc.expression = parseExpression(); col.constraints.add(dc); }
            else if (match(TokenType.CHECK)) { CheckColumnConstraint cc = new CheckColumnConstraint(); expect(TokenType.LPAREN); cc.expression = parseExpression(); expect(TokenType.RPAREN); col.constraints.add(cc); }
            else if (match(TokenType.COLLATE)) { CollateColumnConstraint cc = new CollateColumnConstraint(); cc.collationName = expectName(); col.constraints.add(cc); }
            else if (match(TokenType.REFERENCES)) { ForeignKeyColumnConstraint fk = new ForeignKeyColumnConstraint(); fk.foreignTable = expectName(); col.constraints.add(fk); }
            else break;
        }
        return col;
    }

    private TypeName parseTypeName() throws ParseException {
        TypeName tn = new TypeName(); tn.name = expectName();
        while (check(TokenType.IDENTIFIER) && !check(TokenType.LPAREN)) tn.name += " " + advance().value;
        if (match(TokenType.LPAREN)) { tn.size1 = parseExpression(); if (match(TokenType.COMMA)) tn.size2 = parseExpression(); expect(TokenType.RPAREN); }
        return tn;
    }

    private TableConstraint parseTableConstraint() throws ParseException {
        String cn = null; if (match(TokenType.CONSTRAINT)) cn = expectName();
        if (check(TokenType.PRIMARY)) { PrimaryKey pk = new PrimaryKey(); pk.name = cn; expect(TokenType.PRIMARY); expect(TokenType.KEY); expect(TokenType.LPAREN); pk.columns = new ArrayList<>(); do { pk.columns.add(parseIndexedColumn()); } while (match(TokenType.COMMA)); expect(TokenType.RPAREN); if (match(TokenType.AUTOINCREMENT)) pk.autoIncrement = true; return pk; }
        if (match(TokenType.UNIQUE)) { Unique uq = new Unique(); uq.name = cn; expect(TokenType.LPAREN); uq.columns = new ArrayList<>(); do { uq.columns.add(parseIndexedColumn()); } while (match(TokenType.COMMA)); expect(TokenType.RPAREN); return uq; }
        if (match(TokenType.CHECK)) { Check ck = new Check(); ck.name = cn; expect(TokenType.LPAREN); ck.expression = parseExpression(); expect(TokenType.RPAREN); return ck; }
        if (match(TokenType.FOREIGN)) { ForeignKey fk = new ForeignKey(); fk.name = cn; expect(TokenType.KEY); expect(TokenType.LPAREN); fk.columns = new ArrayList<>(); do { fk.columns.add(expectName()); } while (match(TokenType.COMMA)); expect(TokenType.RPAREN); expect(TokenType.REFERENCES); fk.foreignTable = expectName(); return fk; }
        throw new ParseException("Expected table constraint");
    }

    private IndexedColumn parseIndexedColumn() throws ParseException {
        IndexedColumn ic = new IndexedColumn(); ic.expression = parseExpression();
        if (match(TokenType.COLLATE)) ic.collationName = expectName();
        if (match(TokenType.ASC)) ic.order = OrderDirection.ASC; else if (match(TokenType.DESC)) ic.order = OrderDirection.DESC;
        return ic;
    }

    private CreateIndexStatement parseCreateIndex() throws ParseException {
        CreateIndexStatement ci = new CreateIndexStatement(); if (match(TokenType.UNIQUE)) ci.unique = true;
        expect(TokenType.INDEX); if (match(TokenType.IF)) { expect(TokenType.NOT); expect(TokenType.EXISTS); ci.ifNotExists = true; }
        ci.indexName = expectName(); if (match(TokenType.DOT)) { ci.databaseName = ci.indexName; ci.indexName = expectName(); }
        expect(TokenType.ON); ci.tableName = expectName(); expect(TokenType.LPAREN); ci.columns = new ArrayList<>();
        do { ci.columns.add(parseIndexedColumn()); } while (match(TokenType.COMMA)); expect(TokenType.RPAREN);
        if (match(TokenType.WHERE)) ci.where = parseExpression();
        return ci;
    }

    private CreateViewStatement parseCreateView(boolean temp) throws ParseException {
        CreateViewStatement cv = new CreateViewStatement(); cv.temporary = temp; expect(TokenType.VIEW);
        if (match(TokenType.IF)) { expect(TokenType.NOT); expect(TokenType.EXISTS); cv.ifNotExists = true; }
        cv.viewName = expectName(); expect(TokenType.AS); cv.select = parseSelect(); return cv;
    }

    private CreateTriggerStatement parseCreateTrigger(boolean temp) throws ParseException {
        CreateTriggerStatement ct = new CreateTriggerStatement(); ct.temporary = temp; expect(TokenType.TRIGGER);
        if (match(TokenType.IF)) { expect(TokenType.NOT); expect(TokenType.EXISTS); ct.ifNotExists = true; }
        ct.triggerName = expectName();
        if (match(TokenType.BEFORE)) ct.triggerTime = TriggerTime.BEFORE; else if (match(TokenType.AFTER)) ct.triggerTime = TriggerTime.AFTER; else if (match(TokenType.INSTEAD)) { expect(TokenType.OF); ct.triggerTime = TriggerTime.INSTEAD_OF; }
        if (match(TokenType.DELETE)) ct.triggerEvent = TriggerEvent.DELETE; else if (match(TokenType.INSERT)) ct.triggerEvent = TriggerEvent.INSERT; else if (match(TokenType.UPDATE)) ct.triggerEvent = TriggerEvent.UPDATE;
        expect(TokenType.ON); ct.tableName = expectName();
        expect(TokenType.BEGIN); ct.body = new ArrayList<>(); while (!check(TokenType.END)) ct.body.add(parseStatement()); expect(TokenType.END);
        return ct;
    }

    // ==================== ALTER ====================
    private AlterTableStatement parseAlterTable() throws ParseException {
        AlterTableStatement at = new AlterTableStatement(); expect(TokenType.ALTER); expect(TokenType.TABLE);
        at.tableName = expectName(); if (match(TokenType.DOT)) { at.databaseName = at.tableName; at.tableName = expectName(); }
        if (match(TokenType.RENAME)) { if (match(TokenType.COLUMN)) { at.alterType = AlterType.RENAME_COLUMN; at.renameColumnOld = expectName(); expect(TokenType.TO); at.renameColumnNew = expectName(); } else { expect(TokenType.TO); at.alterType = AlterType.RENAME_TO; at.newTableName = expectName(); } }
        else if (match(TokenType.ADD)) { match(TokenType.COLUMN); at.alterType = AlterType.ADD_COLUMN; at.addColumn = parseColumnDefinition(); }
        else if (match(TokenType.DROP)) { match(TokenType.COLUMN); at.alterType = AlterType.DROP_COLUMN; at.dropColumnName = expectName(); }
        return at;
    }

    // ==================== DROP ====================
    private Statement parseDrop() throws ParseException {
        expect(TokenType.DROP);
        if (match(TokenType.TABLE)) { DropTable dt = new DropTable(); if (match(TokenType.IF)) { expect(TokenType.EXISTS); dt.ifExists = true; } dt.tableName = expectName(); return dt; }
        if (match(TokenType.INDEX)) { DropIndex di = new DropIndex(); if (match(TokenType.IF)) { expect(TokenType.EXISTS); di.ifExists = true; } di.indexName = expectName(); return di; }
        if (match(TokenType.VIEW)) { DropView dv = new DropView(); if (match(TokenType.IF)) { expect(TokenType.EXISTS); dv.ifExists = true; } dv.viewName = expectName(); return dv; }
        if (match(TokenType.TRIGGER)) { DropTrigger dt = new DropTrigger(); if (match(TokenType.IF)) { expect(TokenType.EXISTS); dt.ifExists = true; } dt.triggerName = expectName(); return dt; }
        throw new ParseException("Expected TABLE, INDEX, VIEW, or TRIGGER after DROP");
    }

    // ==================== Transactions ====================
    private Begin parseBegin() throws ParseException { Begin b = new Begin(); expect(TokenType.BEGIN); match(TokenType.DEFERRED); match(TokenType.IMMEDIATE); match(TokenType.EXCLUSIVE); match(TokenType.TRANSACTION); return b; }
    private Rollback parseRollback() throws ParseException { Rollback rb = new Rollback(); expect(TokenType.ROLLBACK); match(TokenType.TRANSACTION); return rb; }
    private Savepoint parseSavepoint() throws ParseException { Savepoint sp = new Savepoint(); expect(TokenType.SAVEPOINT); sp.savepointName = expectName(); return sp; }
    private Savepoint parseRelease() throws ParseException { Savepoint sp = new Savepoint(); expect(TokenType.RELEASE); match(TokenType.SAVEPOINT); sp.savepointName = expectName(); sp.isRelease = true; return sp; }

    // ==================== PRAGMA ====================
    private PragmaStatement parsePragma() throws ParseException {
        PragmaStatement ps = new PragmaStatement(); expect(TokenType.PRAGMA); ps.pragmaName = expectName();
        if (match(TokenType.DOT)) { ps.databaseName = ps.pragmaName; ps.pragmaName = expectName(); }
        if (match(TokenType.EQ)) ps.value = parseExpression();
        else if (check(TokenType.LPAREN)) { advance(); if (!check(TokenType.RPAREN)) ps.value = parseExpression(); expect(TokenType.RPAREN); }
        return ps;
    }
    private ReindexStatement parseReindex() throws ParseException { ReindexStatement ri = new ReindexStatement(); expect(TokenType.REINDEX); if (!isAtEnd() && !check(TokenType.SEMICOLON)) ri.tableName = expectName(); return ri; }
    private AttachStatement parseAttach() throws ParseException { AttachStatement as = new AttachStatement(); expect(TokenType.ATTACH); match(TokenType.DATABASE); as.databasePath = parseExpression(); expect(TokenType.AS); as.databaseName = expectName(); return as; }
    private DetachStatement parseDetach() throws ParseException { DetachStatement ds = new DetachStatement(); expect(TokenType.DETACH); match(TokenType.DATABASE); ds.databaseName = expectName(); return ds; }
    private ExplainStatement parseExplain() throws ParseException { ExplainStatement es = new ExplainStatement(); expect(TokenType.EXPLAIN); if (match(TokenType.QUERY)) expect(TokenType.PLAN); es.statement = parseStatement(); return es; }

    // ==================== Expression Parser ====================
    private Expression parseExpression() throws ParseException { return parseOr(); }

    private Expression parseOr() throws ParseException {
        Expression e = parseAnd();
        while (match(TokenType.OR)) { Binary b = new Binary(); b.left = e; b.operator = BinaryOp.OR; b.right = parseAnd(); e = b; }
        return e;
    }

    private Expression parseAnd() throws ParseException {
        Expression e = parseNot();
        while (match(TokenType.AND)) { Binary b = new Binary(); b.left = e; b.operator = BinaryOp.AND; b.right = parseNot(); e = b; }
        return e;
    }

    private Expression parseNot() throws ParseException {
        if (match(TokenType.NOT)) { Unary u = new Unary(); u.operator = UnaryOp.NOT; u.operand = parseNot(); return u; }
        return parseComparison();
    }

    private Expression parseComparison() throws ParseException {
        Expression e = parseAddition();
        if (check(TokenType.IS)) {
            advance(); boolean not = match(TokenType.NOT);
            if (check(TokenType.NULL)) { advance(); IsNull isn = new IsNull(); isn.expression = e; isn.isNotNull = not; return isn; }
            Binary b = new Binary(); b.left = e; b.operator = not ? BinaryOp.IS_NOT : BinaryOp.IS; b.right = parseAddition(); return b;
        }
        BinaryOp op = null;
        if (match(TokenType.EQ) || match(TokenType.EQ2)) op = BinaryOp.EQ;
        else if (match(TokenType.NEQ)) op = BinaryOp.NEQ;
        else if (match(TokenType.LT)) op = BinaryOp.LT;
        else if (match(TokenType.LTE)) op = BinaryOp.LTE;
        else if (match(TokenType.GT)) op = BinaryOp.GT;
        else if (match(TokenType.GTE)) op = BinaryOp.GTE;
        if (op != null) { Binary b = new Binary(); b.left = e; b.operator = op; b.right = parseAddition(); return b; }
        if (match(TokenType.LIKE)) { Binary b = new Binary(); b.left = e; b.operator = BinaryOp.LIKE; b.right = parseAddition(); return b; }
        if (match(TokenType.GLOB)) { Binary b = new Binary(); b.left = e; b.operator = BinaryOp.GLOB; b.right = parseAddition(); return b; }
        if (match(TokenType.REGEXP)) { Binary b = new Binary(); b.left = e; b.operator = BinaryOp.REGEXP; b.right = parseAddition(); return b; }
        if (check(TokenType.BETWEEN)) { advance(); Between bt = new Between(); bt.expression = e; bt.low = parseAddition(); expect(TokenType.AND); bt.high = parseAddition(); return bt; }
        if (check(TokenType.IN)) { advance(); return parseIn(e, false); }
        return e;
    }

    private Expression parseAddition() throws ParseException {
        Expression e = parseMultiplication();
        while (true) {
            BinaryOp op = null;
            if (match(TokenType.PLUS)) op = BinaryOp.ADD; else if (match(TokenType.MINUS)) op = BinaryOp.SUBTRACT; else if (match(TokenType.CONCAT)) op = BinaryOp.CONCAT; else break;
            Binary b = new Binary(); b.left = e; b.operator = op; b.right = parseMultiplication(); e = b;
        }
        return e;
    }

    private Expression parseMultiplication() throws ParseException {
        Expression e = parseUnary();
        while (true) {
            BinaryOp op = null;
            if (match(TokenType.STAR)) op = BinaryOp.MULTIPLY; else if (match(TokenType.SLASH)) op = BinaryOp.DIVIDE; else if (match(TokenType.PERCENT)) op = BinaryOp.MODULO;
            else if (match(TokenType.AMPERSAND)) op = BinaryOp.BIT_AND; else if (match(TokenType.PIPE)) op = BinaryOp.BIT_OR; else if (match(TokenType.LSHIFT)) op = BinaryOp.LSHIFT; else if (match(TokenType.RSHIFT)) op = BinaryOp.RSHIFT;
            else break;
            Binary b = new Binary(); b.left = e; b.operator = op; b.right = parseUnary(); e = b;
        }
        return e;
    }

    private Expression parseUnary() throws ParseException {
        if (match(TokenType.MINUS)) { Unary u = new Unary(); u.operator = UnaryOp.NEGATE; u.operand = parseUnary(); return u; }
        if (match(TokenType.PLUS)) return parseUnary();
        if (match(TokenType.TILDE)) { Unary u = new Unary(); u.operator = UnaryOp.BIT_NOT; u.operand = parseUnary(); return u; }
        return parsePrimary();
    }

    private Expression parsePrimary() throws ParseException {
        Token t = peek();
        if (match(TokenType.NULL)) { Literal l = new Literal(); l.type = Literal.Type.NULL; return l; }
        if (match(TokenType.INTEGER_LITERAL)) { Literal l = new Literal(); l.type = Literal.Type.INTEGER; l.value = t.value; return l; }
        if (match(TokenType.FLOAT_LITERAL)) { Literal l = new Literal(); l.type = Literal.Type.FLOAT; l.value = t.value; return l; }
        if (match(TokenType.STRING_LITERAL)) { Literal l = new Literal(); l.type = Literal.Type.STRING; l.value = t.value; return l; }
        if (match(TokenType.BLOB_LITERAL)) { Literal l = new Literal(); l.type = Literal.Type.BLOB; l.value = t.value; return l; }
        if (match(TokenType.CURRENT_DATE)) { Literal l = new Literal(); l.type = Literal.Type.CURRENT_DATE; return l; }
        if (match(TokenType.CURRENT_TIME)) { Literal l = new Literal(); l.type = Literal.Type.CURRENT_TIME; return l; }
        if (match(TokenType.CURRENT_TIMESTAMP)) { Literal l = new Literal(); l.type = Literal.Type.CURRENT_TIMESTAMP; return l; }
        if (t.value != null && (t.value.startsWith("?") || t.value.startsWith("$") || t.value.startsWith(":") || t.value.startsWith("@"))) { advance(); Variable v = new Variable(); if (t.value.equals("?")) { paramCounter++; v.name = "?" + paramCounter; } else { v.name = t.value; } return v; }
        if (match(TokenType.EXISTS)) { Exists ex = new Exists(); expect(TokenType.LPAREN); ex.subquery = parseSelect(); expect(TokenType.RPAREN); return ex; }
        if (match(TokenType.CASE)) return parseCase();
        if (match(TokenType.CAST)) { Cast ce = new Cast(); expect(TokenType.LPAREN); ce.expression = parseExpression(); expect(TokenType.AS); ce.typeName = parseTypeName(); expect(TokenType.RPAREN); return ce; }
        if (match(TokenType.RAISE)) { Raise r = new Raise(); expect(TokenType.LPAREN); if (match(TokenType.IGNORE)) r.type = RaiseType.IGNORE; else if (match(TokenType.ROLLBACK)) r.type = RaiseType.ROLLBACK; else if (match(TokenType.ABORT)) r.type = RaiseType.ABORT; else if (match(TokenType.FAIL)) r.type = RaiseType.FAIL; if (check(TokenType.COMMA)) { advance(); r.message = expect(TokenType.STRING_LITERAL).value; } expect(TokenType.RPAREN); return r; }
        if (check(TokenType.LPAREN)) { advance(); if (check(TokenType.SELECT)) { Subquery sq = new Subquery(); sq.subquery = parseSelect(); expect(TokenType.RPAREN); return sq; } Expression expr = parseExpression(); expect(TokenType.RPAREN); return expr; }
        if (check(TokenType.IDENTIFIER) || t.isKeyword()) {
            String name = advance().value;
            if (check(TokenType.LPAREN)) {
                advance(); FunctionCall fc = new FunctionCall(); fc.name = name; fc.distinct = match(TokenType.DISTINCT); fc.arguments = new ArrayList<>();
                if (check(TokenType.STAR)) { advance(); fc.star = true; } else if (!check(TokenType.RPAREN)) { do { fc.arguments.add(parseExpression()); } while (match(TokenType.COMMA)); }
                expect(TokenType.RPAREN);
                if (match(TokenType.FILTER)) { expect(TokenType.LPAREN); expect(TokenType.WHERE); fc.filter = parseExpression(); expect(TokenType.RPAREN); }
                if (match(TokenType.OVER)) { expect(TokenType.LPAREN); match(TokenType.PARTITION); if (match(TokenType.BY)) { do { parseExpression(); } while (match(TokenType.COMMA)); } parseOptionalOrderBy(); expect(TokenType.RPAREN); }
                return fc;
            }
            ColumnRef cr = new ColumnRef();
            if (match(TokenType.DOT)) { cr.tableName = name; cr.columnName = expectName(); } else { cr.columnName = name; }
            return cr;
        }
        throw new ParseException("Unexpected token: " + t.type + "('" + t.value + "')", t.position);
    }

    private Case parseCase() throws ParseException {
        Case ce = new Case(); if (!check(TokenType.WHEN)) ce.operand = parseExpression();
        ce.whenClauses = new ArrayList<>();
        while (match(TokenType.WHEN)) { WhenThen wt = new WhenThen(); wt.when = parseExpression(); expect(TokenType.THEN); wt.then = parseExpression(); ce.whenClauses.add(wt); }
        if (match(TokenType.ELSE)) ce.elseExpression = parseExpression(); expect(TokenType.END); return ce;
    }

    private In parseIn(Expression left, boolean not) throws ParseException {
        In in = new In(); in.expression = left; in.not = not;
        if (check(TokenType.LPAREN)) {
            advance();
            if (check(TokenType.SELECT)) { in.subquery = parseSelect(); } else { in.values = new ArrayList<>(); do { in.values.add(parseExpression()); } while (match(TokenType.COMMA)); }
            expect(TokenType.RPAREN);
        } else { in.tableName = expectName(); }
        return in;
    }
}
