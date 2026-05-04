package com.victor.sql_api.assistant.retrieval.model.domain;

public record RelevantColumn(
        String schemaName,
        String tableName,
        String columnName,
        String dataType,
        String semanticRole,
        String description,
        boolean primaryKey,
        boolean foreignKey,
        double score
) {
}



