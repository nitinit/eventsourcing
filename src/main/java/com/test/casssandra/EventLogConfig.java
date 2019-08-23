package com.test.casssandra;

public class EventLogConfig {

    private final String keySpace;
    private final String tableName;

    public EventLogConfig(String keySpace, String tableName) {
        this.keySpace = keySpace;
        this.tableName = tableName;
    }

    public String getKeySpace() {
        return keySpace;
    }

    public String getTableName() {
        return tableName;
    }

}
