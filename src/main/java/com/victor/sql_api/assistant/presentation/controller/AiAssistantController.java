package com.victor.sql_api.assistant.presentation.controller;

import com.victor.sql_api.assistant.application.model.AiAssistantResult;
import com.victor.sql_api.assistant.application.service.AiAssistantService;
import com.victor.sql_api.shared.api.ApiResponse;
import com.victor.sql_api.shared.api.ResponseMeta;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/assistant")
public class AiAssistantController {

    private final AiAssistantService aiAssistantService;

    public AiAssistantController(AiAssistantService aiAssistantService) {
        this.aiAssistantService = aiAssistantService;
    }

    @PostMapping("/suggest")
    public ResponseEntity<ApiResponse<AiAssistantResult>> suggest(@RequestBody Map<String, Object> request) {
        String prompt = request == null ? null : asString(request.get("prompt"));
        if (prompt == null || prompt.isBlank()) {
            prompt = request == null ? null : asString(request.get("question"));
        }
        String currentSql = request == null ? null : asString(request.get("currentSql"));
        AiAssistantResult result = aiAssistantService.suggest(prompt, currentSql);

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
}
