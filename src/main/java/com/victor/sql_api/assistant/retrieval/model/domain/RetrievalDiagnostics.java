package com.victor.sql_api.assistant.retrieval.model.domain;

import java.util.List;
import java.util.Set;

public record RetrievalDiagnostics(
        Set<String> tokens,
        String queryUnderstandingEngine,
        double queryUnderstandingConfidence,
        boolean queryUnderstandingFallbackApplied,
        String queryUnderstandingFallbackReason,
        int totalTablesScanned,
        int totalColumnsScanned,
        int totalRelationshipsScanned,
        int selectedTables,
        int selectedColumns,
        int selectedRelationships,
        List<String> tableScoringDetails,
        List<String> columnScoringDetails,
        List<String> relationshipResolutionDetails
) {
}


