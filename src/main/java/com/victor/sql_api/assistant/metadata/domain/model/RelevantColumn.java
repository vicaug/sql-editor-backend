package com.victor.sql_api.assistant.metadata.domain.model;

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
