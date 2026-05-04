package com.victor.sql_api.assistant.nl2sql.domain.model;

import java.util.List;

public record QueryUnderstanding(
        QueryIntent intent,
        String domainHint,
        List<String> metrics,
        List<String> dimensions,
        List<String> filters,
        List<String> timeHints,
        boolean requiresAggregation,
        boolean requiresJoin,
        double confidence
) {
}
