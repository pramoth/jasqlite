package com.jasqlite.vdbe;

/**
 * VDBE Opcodes - analogous to SQLite opcodes.
 */
public enum Opcode {
    // Control flow
    Halt,           // Halt execution
    Goto,           // Unconditional jump
    Gosub,          // Jump to subroutine
    Return,         // Return from subroutine
    Init,           // Initialize and jump
    Noop,           // No operation
    
    // Loading values
    Integer,        // Load integer constant
    Real,           // Load real constant
    String8,        // Load string constant
    Null,           // Load NULL
    Blob,           // Load blob constant
    Variable,       // Load bound variable
    Copy,           // Copy register
    SCopy,          // Shallow copy register
    
    // Arithmetic
    Add,            // Addition
    Subtract,       // Subtraction
    Multiply,       // Multiplication
    Divide,         // Division
    Remainder,      // Modulo
    Concat,         // String concatenation
    
    // Bitwise
    BitAnd,         // Bitwise AND
    BitOr,          // Bitwise OR
    ShiftLeft,      // Left shift
    ShiftRight,     // Right shift
    BitNot,         // Bitwise NOT
    
    // Logical
    Not,            // Logical NOT
    
    // Comparison
    Eq,             // Equal
    Ne,             // Not equal
    Lt,             // Less than
    Le,             // Less or equal
    Gt,             // Greater than
    Ge,             // Greater or equal
    If,             // If true jump
    IfNot,          // If false jump
    IsNull,         // If null jump
    NotNull,        // If not null jump
    Compare,        // Compare two registers
    Jump,           // Jump based on comparison
    
    // String functions
    Like,           // LIKE operator
    Glob,           // GLOB operator
    RegExp,         // REGEXP operator
    
    // Table access
    Column,         // Read column value
    Rowid,          // Read rowid
    MakeRecord,     // Create a record
    
    // Cursor management
    OpenRead,       // Open cursor for reading
    OpenWrite,      // Open cursor for writing
    Close,          // Close cursor
    CloseAll,
    
    // Cursor movement
    Rewind,         // Move to first row
    Last,           // Move to last row
    Next,           // Move to next row
    Prev,           // Move to previous row
    
    // Seek operations
    SeekGE,         // Seek >=
    SeekGT,         // Seek >
    SeekLE,         // Seek <=
    SeekLT,         // Seek <
    SeekHit,        // Seek hit
    
    // DML
    NewRowid,       // Generate new rowid
    Insert,         // Insert row
    Delete,         // Delete row
    
    // Aggregation
    AggStep,        // Add to aggregate
    AggFinal,       // Finalize aggregate
    
    // Sorting
    Sort,           // Sort
    SorterSort,     // Sorter sort
    SorterNext,     // Sorter next
    SorterInsert,   // Sorter insert
    SorterData,     // Sorter data
    
    // Functions
    Function,       // Call function
    
    // Result
    ResultRow,      // Output result row
    
    // Schema operations
    Transaction,    // Begin transaction
    ReadCookie,     // Read schema cookie
    SetCookie,      // Set schema cookie
    ParseSchema,    // Parse schema
    
    // DDL
    CreateTable,    // Create table
    CreateIndex,    // Create index
    Destroy,        // Drop table/index
    Clear,          // Clear table
    
    // Integrity
    IntegrityCk,    // Integrity check
    
    // Miscellaneous
    AutoCommit,     // Set autocommit
    Trace,          // Trace
    
    // Index
    IdxInsert,      // Insert into index
    IdxDelete,      // Delete from index
    IdxRowid,       // Get rowid from index
    IdxLT,          // Index seek <
    IdxGE,          // Index seek >=
    
    // Sequence
    Sequence,       // Get autoincrement value
    NewRowId,       // Generate new rowid (alias)
    
    // Collation
    CollSeq,        // Push collation sequence
    
    // Subquery
    OpenEphemeral,  // Open ephemeral table
    OpenAutoindex,  // Open autoindex
    
    // Affinity
    Affinity,       // Apply column affinity
    
    // Check
    Check,          // Check constraint
    
    // Journal
    JournalMode     // Set journal mode
}
