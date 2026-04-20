package com.victor.sql_api.assistant.metadata.domain.model;

public record RelevantTable(
        String schemaName,
        String tableName,
        String businessDescription,
        double score
) {
}
