package com.victor.sql_api.assistant.presentation.controller;

import com.victor.sql_api.assistant.nl2sql.model.AiAssistantResult;
import com.victor.sql_api.assistant.llm.router.LlmProviderRouter;
import com.victor.sql_api.assistant.metadata_retrieval.router.MetadataContextProviderRouter;
import com.victor.sql_api.assistant.metadata_retrieval.model.RetrievalConstraints;
import com.victor.sql_api.assistant.metadata_retrieval.model.RetrievalRequest;
import com.victor.sql_api.assistant.sql_guard.application.SqlGuardService;
import com.victor.sql_api.assistant.nl2sql.model.Nl2SqlContext;
import com.victor.sql_api.assistant.nl2sql.presentation.model.SqlValidationRequest;
import com.victor.sql_api.assistant.nl2sql.presentation.model.SqlValidationResponse;
import com.victor.sql_api.shared.api.ApiResponse;
import com.victor.sql_api.shared.api.ResponseMeta;
import com.victor.sql_api.shared.exception.BadRequestException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/assistant")
public class AiAssistantController {

    private final LlmProviderRouter llmProviderRouter;
    private final MetadataContextProviderRouter metadataContextRouterService;
    private final SqlGuardService sqlGuardService;

    public AiAssistantController(
            LlmProviderRouter llmProviderRouter,
            MetadataContextProviderRouter metadataContextRouterService,
            SqlGuardService sqlGuardService
    ) {
        this.llmProviderRouter = llmProviderRouter;
        this.metadataContextRouterService = metadataContextRouterService;
        this.sqlGuardService = sqlGuardService;
    }

    @PostMapping("/text-to-sql-query")
    public ResponseEntity<ApiResponse<AiAssistantResult>> textToSqlQuery(@RequestBody Map<String, Object> request) {
        return buildSuggestionResponse(request);
    }

    private ResponseEntity<ApiResponse<AiAssistantResult>> buildSuggestionResponse(Map<String, Object> request) {
        String prompt = request == null ? null : asString(request.get("prompt"));
        if (prompt == null || prompt.isBlank()) {
            prompt = request == null ? null : asString(request.get("question"));
        }
        String currentSql = request == null ? null : asString(request.get("currentSql"));
        String provider = request == null ? null : asString(request.get("provider"));
        String metadataProvider = request == null ? null : asString(request.get("metadataProvider"));
        String queryUnderstandingEngine = request == null ? null : asString(request.get("queryUnderstandingEngine"));
        boolean enableSqlGuard = request == null || asBoolean(request.get("enableSqlGuard"), true);
        AiAssistantResult result = llmProviderRouter.suggest(
                prompt,
                currentSql,
                provider,
                enableSqlGuard,
                metadataProvider,
                queryUnderstandingEngine
        );

        return ResponseEntity.ok(
                ApiResponse.success(
                        result,
                        new ResponseMeta(Instant.now(), UUID.randomUUID().toString())
                )
        );
    }

    @PostMapping("/nl2sql/validate")
    public ResponseEntity<ApiResponse<SqlValidationResponse>> validateNl2sql(@RequestBody SqlValidationRequest request) {
        if (request == null || request.question() == null || request.question().isBlank()) {
            throw new BadRequestException("NL2SQL_QUESTION_EMPTY", "A pergunta para validacao NL2SQL nao pode ser vazia.");
        }
        Nl2SqlContext context = metadataContextRouterService.buildContext(
                new RetrievalRequest(request.question(), RetrievalConstraints.defaults(), null),
                "askdata_like"
        );
        var validation = sqlGuardService.validate(request.sql(), context.metadataContext(), context.queryUnderstanding());
        SqlValidationResponse response = new SqlValidationResponse(validation, context, context.metadataContext().diagnostics());
        return ResponseEntity.ok(ApiResponse.success(response, new ResponseMeta(Instant.now(), UUID.randomUUID().toString())));
    }

    private String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private boolean asBoolean(Object value, boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        String text = String.valueOf(value).trim();
        if (text.isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(text);
    }
}








