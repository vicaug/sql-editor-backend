package com.victor.sql_api.assistant.metadata.model.context;

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









