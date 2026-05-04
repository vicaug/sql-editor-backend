package com.victor.sql_api.assistant.nl2sql.domain.model;

import com.victor.sql_api.assistant.metadata.domain.model.MetadataContext;

public record Nl2SqlContext(
        String originalQuestion,
        QueryUnderstanding queryUnderstanding,
        MetadataContext metadataContext,
        String queryUnderstandingEngine,
        double queryUnderstandingConfidence,
        boolean queryUnderstandingFallbackApplied,
        String queryUnderstandingFallbackReason,
        String promptContext
) {
}
