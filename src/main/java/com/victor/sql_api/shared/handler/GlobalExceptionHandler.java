package com.victor.sql_api.shared.handler;

import com.victor.sql_api.shared.api.ApiError;
import com.victor.sql_api.shared.api.ApiResponse;
import com.victor.sql_api.shared.api.ResponseMeta;
import com.victor.sql_api.shared.exception.ApiException;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.UUID;

@RestControllerAdvice
public class GlobalExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ApiResponse<Void>> handleApiException(ApiException ex, HttpServletRequest request) {
        log.warn("API error at {} [{}]: {}", request.getRequestURI(), ex.getCode(), ex.getMessage());
        ApiError error = new ApiError(ex.getCode(), ex.getMessage(), request.getRequestURI());
        return ResponseEntity
                .status(ex.getStatus())
                .body(ApiResponse.error(error, meta()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Void>> handleGenericException(Exception ex, HttpServletRequest request) {
        log.error("Unexpected error at {}", request.getRequestURI(), ex);
        ApiError error = new ApiError(
                "INTERNAL_SERVER_ERROR",
                "Ocorreu um erro inesperado ao processar a requisição.",
                request.getRequestURI()
        );
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.error(error, meta()));
    }

    private ResponseMeta meta() {
        return new ResponseMeta(Instant.now(), UUID.randomUUID().toString());
    }
}








