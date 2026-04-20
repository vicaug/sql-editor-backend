package com.victor.sql_api.shared.exception;

import org.springframework.http.HttpStatus;

public class FeatureNotImplementedException extends ApiException {
    public FeatureNotImplementedException(String code, String message) {
        super(code, message, HttpStatus.NOT_IMPLEMENTED);
    }
}
