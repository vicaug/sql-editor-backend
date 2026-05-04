package com.victor.sql_api.assistant.nl2sql.domain.model;

import java.util.List;

public record SqlValidationResult(
        boolean valid,
        List<String> errors,
        List<String> warnings,
        List<String> detectedTables,
        List<String> detectedColumns
) {
}
