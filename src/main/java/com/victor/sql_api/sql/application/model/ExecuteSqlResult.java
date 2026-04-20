package com.victor.sql_api.sql.application.model;

import com.victor.sql_api.sql.domain.model.SqlStatementType;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record ExecuteSqlResult(
        String executionId,
        Instant executedAt,
        long durationMs,
        SqlStatementType statementType,
        List<String> columns,
        List<Map<String, Object>> rows,
        long rowCount,
        Long affectedRows,
        boolean truncated,
        String message,
        Pagination pagination
) {
    public record Pagination(
            int page,
            int size,
            boolean hasNext,
            Long totalRows
    ) {
    }
}
