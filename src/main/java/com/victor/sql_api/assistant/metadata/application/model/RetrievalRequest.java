package com.victor.sql_api.assistant.metadata.application.model;

public record RetrievalRequest(
        String question,
        RetrievalConstraints constraints
) {
    public RetrievalConstraints effectiveConstraints() {
        return constraints == null ? RetrievalConstraints.defaults() : constraints;
    }
}
