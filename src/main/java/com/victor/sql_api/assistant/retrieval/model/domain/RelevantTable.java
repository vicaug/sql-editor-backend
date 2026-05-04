package com.victor.sql_api.assistant.retrieval.model.domain;

public record RelevantTable(
        String schemaName,
        String tableName,
        String businessDescription,
        double score
) {
}


