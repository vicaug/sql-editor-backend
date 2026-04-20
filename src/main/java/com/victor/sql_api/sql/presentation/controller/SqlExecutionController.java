package com.victor.sql_api.sql.presentation.controller;

import com.victor.sql_api.shared.api.ApiResponse;
import com.victor.sql_api.shared.api.ResponseMeta;
import com.victor.sql_api.sql.application.model.ExecuteSqlResult;
import com.victor.sql_api.sql.application.service.ExecuteSqlService;
import com.victor.sql_api.shared.exception.BadRequestException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/sql")
public class SqlExecutionController {

    private final ExecuteSqlService executeSqlService;

    public SqlExecutionController(ExecuteSqlService executeSqlService) {
        this.executeSqlService = executeSqlService;
    }

    @PostMapping("/run")
    public ResponseEntity<ApiResponse<ExecuteSqlResult>> run(@RequestBody Map<String, Object> request) {
        String sql = request == null ? null : asString(request.get("sql"));
        Integer page = request == null ? null : asInteger(request.get("page"));
        Integer size = request == null ? null : asInteger(request.get("size"));
        ExecuteSqlResult result = executeSqlService.execute(sql, page, size);

        return ResponseEntity.ok(
                ApiResponse.success(
                        result,
                        new ResponseMeta(Instant.now(), UUID.randomUUID().toString())
                )
        );
    }

    private String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private Integer asInteger(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value).trim());
        } catch (NumberFormatException ex) {
            throw new BadRequestException("INVALID_INTEGER_FIELD", "Campos numéricos inválidos em /sql/run.");
        }
    }
}
