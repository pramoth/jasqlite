package com.jasqlite.vdbe;

import com.jasqlite.util.SQLiteValue;

/**
 * A single VDBE instruction.
 */
public class Instruction {
    public final Opcode opcode;
    public int p1;
    public int p2;
    public int p3;
    public final SQLiteValue p4;
    public final String comment;
    
    public Instruction(Opcode opcode, int p1, int p2, int p3, SQLiteValue p4, String comment) {
        this.opcode = opcode;
        this.p1 = p1;
        this.p2 = p2;
        this.p3 = p3;
        this.p4 = p4;
        this.comment = comment;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%-15s %4d %4d %4d", opcode.name(), p1, p2, p3));
        if (p4 != null) sb.append(" ").append(p4);
        if (comment != null) sb.append("  ; ").append(comment);
        return sb.toString();
    }
}
