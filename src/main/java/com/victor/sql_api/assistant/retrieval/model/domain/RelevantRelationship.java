package com.victor.sql_api.assistant.retrieval.model.domain;

public record RelevantRelationship(
        String fromSchema,
        String fromTable,
        String fromColumn,
        String toSchema,
        String toTable,
        String toColumn,
        String relationshipType,
        double confidence
) {
}


