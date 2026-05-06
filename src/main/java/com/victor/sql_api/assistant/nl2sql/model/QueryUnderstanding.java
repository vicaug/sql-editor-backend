package com.victor.sql_api.assistant.nl2sql.model;

import java.util.List;

public record QueryUnderstanding(
        QueryIntent intent,
        String domainHint,
        List<String> metrics,
        List<String> dimensions,
        List<String> filters,
        List<String> timeHints,
        Boolean requiresAggregation,
        Boolean requiresJoin,
        double confidence
) {
}








