package com.victor.sql_api.assistant.metadata_retrieval.model;

public record RetrievalConstraints(
        int maxTables,
        int maxColumnsPerTable,
        int maxRelationships,
        int maxTotalColumns
) {
    public RetrievalConstraints normalized() {
        RetrievalConstraints defaults = defaults();
        int normalizedMaxTables = maxTables > 0 ? maxTables : defaults.maxTables();
        int normalizedMaxColumnsPerTable = maxColumnsPerTable > 0 ? maxColumnsPerTable : defaults.maxColumnsPerTable();
        int normalizedMaxRelationships = maxRelationships > 0 ? maxRelationships : defaults.maxRelationships();
        int normalizedMaxTotalColumns = maxTotalColumns > 0 ? maxTotalColumns : defaults.maxTotalColumns();
        return new RetrievalConstraints(
                normalizedMaxTables,
                normalizedMaxColumnsPerTable,
                normalizedMaxRelationships,
                normalizedMaxTotalColumns
        );
    }

    public static RetrievalConstraints defaults() {
        return new RetrievalConstraints(
            12,
            20,
            12,
            24
        );
    }
}





