package com.victor.sql_api.sql.application.service;

import com.victor.sql_api.shared.exception.BadRequestException;
import com.victor.sql_api.sql.application.model.ExecuteSqlResult;
import com.victor.sql_api.sql.domain.model.SqlExecutionData;
import com.victor.sql_api.sql.domain.model.SqlStatementType;
import com.victor.sql_api.sql.domain.port.SqlExecutionGateway;
import com.victor.sql_api.sql.infrastructure.config.SqlExecutionProperties;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class ExecuteSqlService {

    private final SqlExecutionGateway sqlExecutionGateway;
    private final SqlExecutionProperties sqlExecutionProperties;

    public ExecuteSqlService(SqlExecutionGateway sqlExecutionGateway, SqlExecutionProperties sqlExecutionProperties) {
        this.sqlExecutionGateway = sqlExecutionGateway;
        this.sqlExecutionProperties = sqlExecutionProperties;
    }

    public ExecuteSqlResult execute(String sqlInput, Integer pageInput, Integer sizeInput) {
        String sql = sqlInput == null ? "" : sqlInput.trim();
        if (sql.isEmpty()) {
            throw new BadRequestException("SQL_EMPTY", "O comando SQL não pode ser vazio.");
        }

        long start = System.currentTimeMillis();
        SqlExecutionData rawResult = sqlExecutionGateway.execute(sql);
        long durationMs = System.currentTimeMillis() - start;

        List<Map<String, Object>> rows = rawResult.rows();
        ExecuteSqlResult.Pagination pagination = null;

        if (rawResult.statementType() == SqlStatementType.QUERY) {
            int page = pageInput != null && pageInput >= 0 ? pageInput : 0;
            int size = sizeInput != null && sizeInput > 0
                    ? Math.min(sizeInput, sqlExecutionProperties.getMaxRows())
                    : Math.min(sqlExecutionProperties.getDefaultPageSize(), sqlExecutionProperties.getMaxRows());

            int from = Math.min(page * size, rows.size());
            int to = Math.min(from + size, rows.size());
            rows = new ArrayList<>(rows.subList(from, to));

            boolean hasNext = to < rawResult.rows().size() || rawResult.truncated();
            pagination = new ExecuteSqlResult.Pagination(page, size, hasNext, null);
        }

        return new ExecuteSqlResult(
                UUID.randomUUID().toString(),
                Instant.now(),
                durationMs,
                rawResult.statementType(),
                rawResult.columns(),
                rows,
                rows.size(),
                rawResult.affectedRows(),
                rawResult.truncated(),
                rawResult.message(),
                pagination
        );
    }
}
