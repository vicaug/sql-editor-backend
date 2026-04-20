package com.victor.sql_api.assistant.application.model;

import com.victor.sql_api.assistant.metadata.domain.model.MetadataContext;

import java.time.Instant;

public record AiAssistantResult(
        String suggestion,
        Instant generatedAt,
        MetadataContext metadataContext
) {
}
