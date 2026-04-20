package com.victor.sql_api.shared.exception;

import org.springframework.http.HttpStatus;

public class SqlExecutionFailureException extends ApiException {
    public SqlExecutionFailureException(String message) {
        super("SQL_EXECUTION_ERROR", message, HttpStatus.BAD_REQUEST);
    }
}
