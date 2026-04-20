package com.victor.sql_api.sql.domain.model;

import java.util.List;
import java.util.Map;

public record SqlExecutionData(
        SqlStatementType statementType,
        List<String> columns,
        List<Map<String, Object>> rows,
        Long affectedRows,
        boolean truncated,
        String message
) {
}
