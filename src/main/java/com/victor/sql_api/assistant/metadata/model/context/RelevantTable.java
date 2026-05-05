package com.victor.sql_api.assistant.metadata.model.context;

public record RelevantTable(
        String schemaName,
        String tableName,
        String businessDescription,
        double score
) {
}









