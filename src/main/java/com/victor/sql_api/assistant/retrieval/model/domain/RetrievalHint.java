package com.victor.sql_api.assistant.retrieval.model.domain;

public record RetrievalHint(
        String type,
        String value,
        double confidence
) {
}


