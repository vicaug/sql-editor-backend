package com.victor.sql_api.shared.api;

public record ApiError(
        String code,
        String message,
        String path
) {
}
