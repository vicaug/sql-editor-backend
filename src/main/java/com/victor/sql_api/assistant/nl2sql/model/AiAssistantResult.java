package com.victor.sql_api.assistant.nl2sql.model;

import com.victor.sql_api.assistant.metadata.model.context.MetadataContext;

import java.time.Instant;

public record AiAssistantResult(
        String suggestion,
        Instant generatedAt,
        MetadataContext metadataContext,
        SqlValidationResult sqlValidation
) {
}







