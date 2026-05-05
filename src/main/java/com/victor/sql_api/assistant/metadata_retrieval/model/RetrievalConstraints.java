package com.victor.sql_api.assistant.metadata_retrieval.model;

public record RetrievalConstraints(
        int maxTables,
        int maxColumnsPerTable,
        int maxRelationships,
        int maxTotalColumns
) {
    public static RetrievalConstraints defaults() {
        return new RetrievalConstraints(
            12, 
            20, 
            12, 
            24
        );
    }
}








