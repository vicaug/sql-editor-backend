package com.victor.sql_api.sql.infrastructure.gateway;

import com.victor.sql_api.shared.exception.SqlExecutionFailureException;
import com.victor.sql_api.sql.domain.model.SqlExecutionData;
import com.victor.sql_api.sql.domain.model.SqlStatementType;
import com.victor.sql_api.sql.domain.port.SqlExecutionGateway;
import com.victor.sql_api.sql.infrastructure.config.SqlExecutionProperties;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
public class JdbcSqlExecutionGateway implements SqlExecutionGateway {

    private final DataSource dataSource;
    private final SqlExecutionProperties sqlExecutionProperties;

    public JdbcSqlExecutionGateway(DataSource dataSource, SqlExecutionProperties sqlExecutionProperties) {
        this.dataSource = dataSource;
        this.sqlExecutionProperties = sqlExecutionProperties;
    }

    @Override
    public SqlExecutionData execute(String sql) {
        try (
                Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()
        ) {
            statement.setQueryTimeout(sqlExecutionProperties.getQueryTimeoutSeconds());
            statement.setMaxRows(sqlExecutionProperties.getMaxRows() + 1);

            boolean hasResultSet = statement.execute(sql);
            if (hasResultSet) {
                return mapQueryResult(sql, statement.getResultSet());
            }

            int updatedRows = statement.getUpdateCount();
            SqlStatementType statementType = resolveStatementType(sql, false);
            return new SqlExecutionData(
                    statementType,
                    List.of(),
                    List.of(),
                    (long) updatedRows,
                    false,
                    "Comando SQL executado com sucesso."
            );
        } catch (SQLException ex) {
            throw new SqlExecutionFailureException(ex.getMessage());
        }
    }

    private SqlExecutionData mapQueryResult(String sql, ResultSet resultSet) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnsCount = metaData.getColumnCount();

        List<String> columns = new ArrayList<>();
        for (int i = 1; i <= columnsCount; i++) {
            columns.add(metaData.getColumnLabel(i));
        }

        while (resultSet.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (String column : columns) {
                row.put(column, resultSet.getObject(column));
            }
            rows.add(row);
        }

        boolean truncated = rows.size() > sqlExecutionProperties.getMaxRows();
        if (truncated) {
            rows = new ArrayList<>(rows.subList(0, sqlExecutionProperties.getMaxRows()));
        }

        return new SqlExecutionData(
                resolveStatementType(sql, true),
                columns,
                rows,
                null,
                truncated,
                truncated
                        ? "Consulta executada com sucesso (resultado truncado pelo limite configurado)."
                        : "Consulta executada com sucesso."
        );
    }

    private SqlStatementType resolveStatementType(String sql, boolean hasResultSet) {
        if (hasResultSet) {
            return SqlStatementType.QUERY;
        }

        String normalized = sql.trim().toUpperCase(Locale.ROOT);
        if (normalized.startsWith("UPDATE")
                || normalized.startsWith("INSERT")
                || normalized.startsWith("DELETE")
                || normalized.startsWith("MERGE")) {
            return SqlStatementType.UPDATE;
        }
        if (normalized.startsWith("CREATE")
                || normalized.startsWith("ALTER")
                || normalized.startsWith("DROP")
                || normalized.startsWith("TRUNCATE")) {
            return SqlStatementType.DDL;
        }
        return SqlStatementType.UNKNOWN;
    }
}
