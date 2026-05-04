package com.victor.sql_api.assistant.application.model;

import com.victor.sql_api.assistant.retrieval.model.domain.MetadataContext;
import com.victor.sql_api.assistant.nl2sql.domain.model.SqlValidationResult;

import java.time.Instant;

public record AiAssistantResult(
        String suggestion,
        Instant generatedAt,
        MetadataContext metadataContext,
        SqlValidationResult sqlValidation
) {
}


