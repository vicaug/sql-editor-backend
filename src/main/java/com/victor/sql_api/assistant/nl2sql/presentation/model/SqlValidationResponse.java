package com.victor.sql_api.assistant.nl2sql.presentation.model;

import com.victor.sql_api.assistant.retrieval.model.domain.RetrievalDiagnostics;
import com.victor.sql_api.assistant.nl2sql.domain.model.Nl2SqlContext;
import com.victor.sql_api.assistant.nl2sql.domain.model.SqlValidationResult;

public record SqlValidationResponse(
        SqlValidationResult validation,
        Nl2SqlContext context,
        RetrievalDiagnostics diagnostics
) {
}



