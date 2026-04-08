package com.jasqlite.sql.ast;

import java.util.List;

public class CreateTriggerStatement extends Statement {
    public boolean temporary;
    public boolean ifNotExists;
    public String triggerName;
    public String databaseName;
    public Enums.TriggerTime triggerTime;
    public Enums.TriggerEvent triggerEvent;
    public String tableName;
    public List<String> updateColumns;
    public boolean forEachRow;
    public Expression whenCondition;
    public List<Statement> body;
}
