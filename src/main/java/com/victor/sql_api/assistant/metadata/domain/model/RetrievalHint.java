package com.victor.sql_api.assistant.metadata.domain.model;

public record RetrievalHint(
        String type,
        String value,
        double confidence
) {
}
