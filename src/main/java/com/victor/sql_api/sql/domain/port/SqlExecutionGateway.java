package com.victor.sql_api.sql.domain.port;

import com.victor.sql_api.sql.domain.model.SqlExecutionData;

public interface SqlExecutionGateway {
    SqlExecutionData execute(String sql);
}
