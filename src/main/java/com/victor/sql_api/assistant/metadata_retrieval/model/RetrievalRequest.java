package com.victor.sql_api.assistant.metadata_retrieval.model;

public record RetrievalRequest(
        String question,
        RetrievalConstraints constraints,
        String queryUnderstandingEngine
) {
    public RetrievalConstraints effectiveConstraints() {
        return constraints == null ? RetrievalConstraints.defaults() : constraints;
    }
}








