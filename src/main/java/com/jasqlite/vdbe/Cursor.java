package com.jasqlite.vdbe;

/**
 * VDBE Cursor - points to a position in a B-tree.
 */
public class Cursor {
    public int cursorId;
    public int rootPage;
    public boolean writable;
    public String tableName;
    public int currentPage;
    public int currentCell;
    public boolean eof;
    public boolean initialized;
    
    public Cursor(int cursorId, int rootPage, boolean writable) {
        this.cursorId = cursorId;
        this.rootPage = rootPage;
        this.writable = writable;
        this.currentPage = rootPage;
        this.currentCell = 0;
        this.eof = false;
        this.initialized = false;
    }
    
    public void reset() {
        this.currentPage = rootPage;
        this.currentCell = 0;
        this.eof = false;
        this.initialized = false;
    }
}
