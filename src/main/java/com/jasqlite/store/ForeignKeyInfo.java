package com.jasqlite.store;

import com.jasqlite.sql.ast.Enums;
import java.util.List;

public class ForeignKeyInfo {
    public List<String> columns;
    public String foreignTable;
    public List<String> foreignColumns;
    public Enums.ForeignKeyAction onDelete;
    public Enums.ForeignKeyAction onUpdate;
}
