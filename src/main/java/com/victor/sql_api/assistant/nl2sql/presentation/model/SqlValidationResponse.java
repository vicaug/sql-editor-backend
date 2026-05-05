package com.victor.sql_api.assistant.nl2sql.presentation.model;

import com.victor.sql_api.assistant.metadata.model.context.RetrievalDiagnostics;
import com.victor.sql_api.assistant.nl2sql.model.Nl2SqlContext;
import com.victor.sql_api.assistant.nl2sql.model.SqlValidationResult;

public record SqlValidationResponse(
        SqlValidationResult validation,
        Nl2SqlContext context,
        RetrievalDiagnostics diagnostics
) {
}








