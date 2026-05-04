package com.victor.sql_api.assistant.nl2sql.domain.model;

public record QueryUnderstandingDecision(
        QueryUnderstanding understanding,
        String selectedEngine,
        boolean fallbackApplied,
        String fallbackReason
) {
}



