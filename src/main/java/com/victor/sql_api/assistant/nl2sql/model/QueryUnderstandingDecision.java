package com.victor.sql_api.assistant.nl2sql.model;

public record QueryUnderstandingDecision(
        QueryUnderstanding understanding,
        String selectedEngine,
        boolean fallbackApplied,
        String fallbackReason
) {
}









