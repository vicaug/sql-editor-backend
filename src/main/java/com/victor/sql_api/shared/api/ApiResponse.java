package com.victor.sql_api.shared.api;

public record ApiResponse<T>(
        boolean success,
        T data,
        ApiError error,
        ResponseMeta meta
) {
    public static <T> ApiResponse<T> success(T data, ResponseMeta meta) {
        return new ApiResponse<>(true, data, null, meta);
    }

    public static <T> ApiResponse<T> error(ApiError error, ResponseMeta meta) {
        return new ApiResponse<>(false, null, error, meta);
    }
}
