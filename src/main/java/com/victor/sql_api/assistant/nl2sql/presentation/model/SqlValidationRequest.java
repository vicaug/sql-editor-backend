package com.victor.sql_api.assistant.nl2sql.presentation.model;

public record SqlValidationRequest(
        String question,
        String sql
) {
}



