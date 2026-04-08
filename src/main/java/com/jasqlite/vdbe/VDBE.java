package com.jasqlite.vdbe;

import com.jasqlite.util.SQLiteValue;
import java.util.*;

/**
 * Virtual Database Engine - executes bytecode programs.
 * Analogous to SQLite's VDBE.
 */
public class VDBE {
    
    private final List<Instruction> program;
    private int pc; // program counter
    private final List<SQLiteValue> registers;
    private final Stack<Cursor> cursors;
    private final List<String> columnNames;
    private boolean halted;
    private SQLiteValue resultRow;
    private List<SQLiteValue[]> resultRows;
    private int changesCount;
    private long lastInsertRowid;
    private int[] compareIndices; // for seek/compare ops
    
    public VDBE() {
        this.program = new ArrayList<>();
        this.registers = new ArrayList<>();
        this.cursors = new Stack<>();
        this.columnNames = new ArrayList<>();
        this.resultRows = new ArrayList<>();
        this.pc = 0;
        this.halted = false;
        this.changesCount = 0;
    }

    // Add an instruction to the program
    public void addInstruction(Opcode opcode, int p1, int p2, int p3, SQLiteValue p4, String comment) {
        program.add(new Instruction(opcode, p1, p2, p3, p4, comment));
    }
    
    public void addInstruction(Opcode opcode, int p1, int p2, int p3) {
        addInstruction(opcode, p1, p2, p3, null, null);
    }
    
    public void addInstruction(Opcode opcode, int p1, int p2, int p3, String comment) {
        addInstruction(opcode, p1, p2, p3, null, comment);
    }

    // Ensure registers up to index exist
    private void ensureRegister(int idx) {
        while (registers.size() <= idx) {
            registers.add(null);
        }
    }
    
    public void setRegister(int idx, SQLiteValue value) {
        ensureRegister(idx);
        registers.set(idx, value);
    }
    
    public SQLiteValue getRegister(int idx) {
        if (idx < 0 || idx >= registers.size()) return SQLiteValue.fromNull();
        SQLiteValue v = registers.get(idx);
        return v != null ? v : SQLiteValue.fromNull();
    }
    
    public void addColumnName(String name) {
        columnNames.add(name);
    }
    
    public List<String> getColumnNames() { return columnNames; }
    
    public void setResultRows(List<SQLiteValue[]> rows) { this.resultRows = rows; }
    public List<SQLiteValue[]> getResultRows() { return resultRows; }
    
    public int getChangesCount() { return changesCount; }
    public void setChangesCount(int count) { this.changesCount = count; }
    public long getLastInsertRowid() { return lastInsertRowid; }
    public void setLastInsertRowid(long rowid) { this.lastInsertRowid = rowid; }
    
    public void pushCursor(Cursor cursor) { cursors.push(cursor); }
    public Cursor popCursor() { return cursors.isEmpty() ? null : cursors.pop(); }
    public Cursor peekCursor() { return cursors.isEmpty() ? null : cursors.peek(); }
    
    public List<Instruction> getProgram() { return program; }
    public void setPc(int pc) { this.pc = pc; }
    
    public void reset() {
        pc = 0;
        halted = false;
        registers.clear();
        cursors.clear();
        resultRows.clear();
    }
    
    public boolean isHalted() { return halted; }
    public void halt() { this.halted = true; }
    
    /**
     * Execute the program. Returns true if a result row was produced.
     */
    public boolean executeStep() {
        if (halted || pc >= program.size()) {
            halted = true;
            return false;
        }
        
        while (pc < program.size() && !halted) {
            Instruction instr = program.get(pc);
            int nextPc = pc + 1;
            
            switch (instr.opcode) {
                case Halt:
                    halted = true;
                    return false;
                    
                case Goto:
                    nextPc = instr.p2;
                    break;
                    
                case Integer:
                    setRegister(instr.p1, SQLiteValue.fromLong(instr.p2));
                    break;
                    
                case Real:
                    setRegister(instr.p1, instr.p4 != null ? instr.p4 : SQLiteValue.fromDouble(0.0));
                    break;
                    
                case String8:
                    setRegister(instr.p1, instr.p4 != null ? instr.p4 : SQLiteValue.fromText(""));
                    break;
                    
                case Null:
                    setRegister(instr.p1, SQLiteValue.fromNull());
                    break;
                    
                case Blob:
                    setRegister(instr.p1, instr.p4 != null ? instr.p4 : SQLiteValue.fromBlob(new byte[0]));
                    break;
                    
                case Variable:
                    // Parameter binding - handled externally
                    break;
                    
                case Copy:
                    setRegister(instr.p1, getRegister(instr.p2));
                    break;
                    
                case SCopy:
                    setRegister(instr.p1, getRegister(instr.p2));
                    break;
                    
                case ResultRow:
                    // Extract result row from registers p1..p1+p2-1
                    SQLiteValue[] row = new SQLiteValue[instr.p2];
                    for (int i = 0; i < instr.p2; i++) {
                        row[i] = getRegister(instr.p1 + i);
                    }
                    resultRows.add(row);
                    pc = nextPc;
                    return true;
                    
                case Add:
                    setRegister(instr.p1, 
                        SQLiteValue.fromDouble(getRegister(instr.p2).asDouble() + getRegister(instr.p3).asDouble()));
                    break;
                    
                case Subtract:
                    setRegister(instr.p1,
                        SQLiteValue.fromDouble(getRegister(instr.p2).asDouble() - getRegister(instr.p3).asDouble()));
                    break;
                    
                case Multiply:
                    setRegister(instr.p1,
                        SQLiteValue.fromDouble(getRegister(instr.p2).asDouble() * getRegister(instr.p3).asDouble()));
                    break;
                    
                case Divide:
                    double divisor = getRegister(instr.p3).asDouble();
                    if (divisor == 0.0) {
                        setRegister(instr.p1, SQLiteValue.fromNull());
                    } else {
                        setRegister(instr.p1,
                            SQLiteValue.fromDouble(getRegister(instr.p2).asDouble() / divisor));
                    }
                    break;
                    
                case Remainder:
                    double mod = getRegister(instr.p3).asDouble();
                    if (mod == 0.0) {
                        setRegister(instr.p1, SQLiteValue.fromNull());
                    } else {
                        setRegister(instr.p1,
                            SQLiteValue.fromDouble(getRegister(instr.p2).asDouble() % mod));
                    }
                    break;
                    
                case Concat: {
                    String left = getRegister(instr.p2).asString();
                    String right = getRegister(instr.p3).asString();
                    if (left == null || right == null) {
                        setRegister(instr.p1, SQLiteValue.fromNull());
                    } else {
                        setRegister(instr.p1, SQLiteValue.fromText(left + right));
                    }
                    break;
                }
                    
                case BitAnd:
                    setRegister(instr.p1, SQLiteValue.fromLong(getRegister(instr.p2).asLong() & getRegister(instr.p3).asLong()));
                    break;
                case BitOr:
                    setRegister(instr.p1, SQLiteValue.fromLong(getRegister(instr.p2).asLong() | getRegister(instr.p3).asLong()));
                    break;
                case ShiftLeft:
                    setRegister(instr.p1, SQLiteValue.fromLong(getRegister(instr.p2).asLong() << getRegister(instr.p3).asLong()));
                    break;
                case ShiftRight:
                    setRegister(instr.p1, SQLiteValue.fromLong(getRegister(instr.p2).asLong() >> getRegister(instr.p3).asLong()));
                    break;
                    
                case Not:
                    setRegister(instr.p1, SQLiteValue.fromLong(getRegister(instr.p2).asLong() == 0 ? 1 : 0));
                    break;
                    
                case BitNot:
                    setRegister(instr.p1, SQLiteValue.fromLong(~getRegister(instr.p2).asLong()));
                    break;
                    
                case Eq:
                    if (getRegister(instr.p1).compareTo(getRegister(instr.p3)) == 0) nextPc = instr.p2;
                    break;
                case Ne:
                    if (getRegister(instr.p1).compareTo(getRegister(instr.p3)) != 0) nextPc = instr.p2;
                    break;
                case Lt:
                    if (getRegister(instr.p1).compareTo(getRegister(instr.p3)) < 0) nextPc = instr.p2;
                    break;
                case Le:
                    if (getRegister(instr.p1).compareTo(getRegister(instr.p3)) <= 0) nextPc = instr.p2;
                    break;
                case Gt:
                    if (getRegister(instr.p1).compareTo(getRegister(instr.p3)) > 0) nextPc = instr.p2;
                    break;
                case Ge:
                    if (getRegister(instr.p1).compareTo(getRegister(instr.p3)) >= 0) nextPc = instr.p2;
                    break;
                    
                case If:
                    if (getRegister(instr.p1).asLong() != 0) nextPc = instr.p2;
                    break;
                case IfNot:
                    if (getRegister(instr.p1).asLong() == 0) nextPc = instr.p2;
                    break;
                case IsNull:
                    if (getRegister(instr.p1).isNull()) nextPc = instr.p2;
                    break;
                case NotNull:
                    if (!getRegister(instr.p1).isNull()) nextPc = instr.p2;
                    break;
                    
                case Compare: {
                    int cmp = getRegister(instr.p1).compareTo(getRegister(instr.p3));
                    setRegister(instr.p1, SQLiteValue.fromLong(cmp));
                    break;
                }
                    
                case Jump: {
                    int cmp = getRegister(instr.p1).asLong() > 0 ? 0 : (getRegister(instr.p1).asLong() < 0 ? 1 : 2);
                    nextPc = new int[]{instr.p2, instr.p3, (int)getRegister(instr.p1 + 1).asLong()}[cmp];
                    break;
                }
                    
                case Column:
                    // Handled by executor context
                    break;
                    
                case MakeRecord: {
                    // Create a record from registers p1..p1+p2-1
                    List<com.jasqlite.util.SQLiteValue> vals = new ArrayList<>();
                    for (int i = 0; i < instr.p2; i++) {
                        vals.add(getRegister(instr.p1 + i));
                    }
                    com.jasqlite.store.record.Record rec = new com.jasqlite.store.record.Record(vals);
                    setRegister(instr.p3, SQLiteValue.fromBlob(rec.serialize()));
                    break;
                }
                    
                case NewRowid:
                    // Generate new rowid - handled by executor
                    break;
                    
                case Insert:
                    changesCount++;
                    break;
                    
                case Delete:
                    changesCount++;
                    break;
                    
                case Rowid:
                    // Handled by executor
                    break;
                    
                case SeekGE:
                case SeekGT:
                case SeekLE:
                case SeekLT:
                    // B-tree seek operations - handled by executor
                    break;
                    
                case Next:
                case Prev:
                    // Cursor advance - handled by executor
                    break;
                    
                case Rewind:
                case Last:
                    // Cursor positioning - handled by executor
                    break;
                    
                case OpenRead:
                case OpenWrite:
                    // Open cursor - handled by executor
                    break;
                    
                case Close:
                    // Close cursor
                    break;
                    
                case AggStep:
                case AggFinal:
                    // Aggregation - handled by executor
                    break;
                    
                case Sort:
                case SorterSort:
                case SorterNext:
                case SorterInsert:
                case SorterData:
                    // Sorting operations
                    break;
                    
                case Function:
                    // Function call - handled by executor
                    break;
                    
                case Init:
                    nextPc = instr.p2;
                    break;
                    
                case Noop:
                    break;
                    
                case Transaction:
                    break;
                    
                case ReadCookie:
                case SetCookie:
                    break;
                    
                case CreateTable:
                case CreateIndex:
                    break;
                    
                case Destroy:
                    break;
                    
                case Clear:
                    break;
                    
                case ParseSchema:
                    break;
                    
                case CloseAll:
                    cursors.clear();
                    break;
                    
                default:
                    // Unknown opcode - skip
                    break;
            }
            
            pc = nextPc;
        }
        
        halted = true;
        return false;
    }
}
