package com.victor.sql_api.assistant.retrieval.model.application;

public record RetrievalRequest(
        String question,
        RetrievalConstraints constraints,
        String queryUnderstandingEngine
) {
    public RetrievalConstraints effectiveConstraints() {
        return constraints == null ? RetrievalConstraints.defaults() : constraints;
    }
}


