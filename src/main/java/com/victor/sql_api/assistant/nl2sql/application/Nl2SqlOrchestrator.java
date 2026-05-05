package com.victor.sql_api.assistant.nl2sql.application;

import com.victor.sql_api.assistant.nl2sql.model.AiAssistantResult;
import com.victor.sql_api.assistant.llm.prompt.TextToSqlPromptTemplate;
import com.victor.sql_api.assistant.llm.provider.AiTextGenerator;
import com.victor.sql_api.assistant.metadata.model.context.MetadataContext;
import com.victor.sql_api.assistant.metadata_retrieval.model.RetrievalRequest;
import com.victor.sql_api.assistant.metadata_retrieval.router.MetadataContextProviderRouter;
import com.victor.sql_api.assistant.nl2sql.model.Nl2SqlContext;
import com.victor.sql_api.assistant.nl2sql.model.SqlValidationResult;
import com.victor.sql_api.assistant.sql_guard.application.SqlGuardService;
import com.victor.sql_api.shared.exception.BadRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Locale;

@Service
public class Nl2SqlOrchestrator {
    private static final Logger log = LoggerFactory.getLogger(Nl2SqlOrchestrator.class);

    private final MetadataContextProviderRouter metadataContextProviderRouter;
    private final SqlGuardService sqlGuardService;

    public Nl2SqlOrchestrator(
            MetadataContextProviderRouter metadataContextProviderRouter,
            SqlGuardService sqlGuardService
    ) {
        this.metadataContextProviderRouter = metadataContextProviderRouter;
        this.sqlGuardService = sqlGuardService;
    }

    public AiAssistantResult suggest(
            String prompt,
            String currentSql,
            boolean enableSqlGuard,
            String metadataProvider,
            String queryUnderstandingEngine,
            AiTextGenerator aiTextGenerator
    ) {
        String question = prompt == null ? "" : prompt.trim();
        if (question.isBlank()) {
            throw new BadRequestException("AI_PROMPT_EMPTY", "O prompt da pergunta em linguagem natural e obrigatorio.");
        }

        Nl2SqlContext nl2SqlContext;
        long metadataStartedAt = System.nanoTime();
        try {
            nl2SqlContext = metadataContextProviderRouter.buildContext(
                    new RetrievalRequest(question, null, queryUnderstandingEngine),
                    metadataProvider
            );
        } catch (BadRequestException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new BadRequestException(
                    "AI_METADATA_CONTEXT_FAILED",
                    "Falha ao montar contexto de metadata para a pergunta. Detalhe: " + defaultText(ex.getMessage(), "erro interno")
            );
        }
        long metadataElapsedMs = (System.nanoTime() - metadataStartedAt) / 1_000_000;
        log.info("AI suggest metadata context built in {} ms", metadataElapsedMs);

        MetadataContext metadata = nl2SqlContext.metadataContext();
        String userPrompt = buildUserPrompt(question, currentSql, nl2SqlContext.promptContext());
        long llmStartedAt = System.nanoTime();
        String rawContent = aiTextGenerator.generate(TextToSqlPromptTemplate.SYSTEM_PROMPT, userPrompt);
        long llmElapsedMs = (System.nanoTime() - llmStartedAt) / 1_000_000;
        log.info("AI suggest LLM generation finished in {} ms", llmElapsedMs);
        String sql = normalizeSqlOutput(rawContent);

        if (sql.isBlank()) {
            throw new BadRequestException("AI_SQL_EMPTY", "A IA nao retornou SQL valido para a solicitacao.");
        }

        SqlValidationResult validation = null;
        if (enableSqlGuard) {
            try {
                validation = sqlGuardService.validate(sql, metadata);
            } catch (Throwable ex) {
                throw new BadRequestException(
                        "AI_SQL_VALIDATION_FAILED",
                        "Falha ao validar SQL gerado pelo guard service: " + defaultText(ex.getMessage(), "erro interno")
                );
            }
            if (!validation.valid()) {
                String detail = validation.errors().isEmpty()
                        ? "SQL rejeitado pelo guard service."
                        : String.join(" | ", validation.errors());
                throw new BadRequestException("AI_SQL_REJECTED", detail);
            }
        }

        return new AiAssistantResult(sql, Instant.now(), metadata, validation);
    }

    private String buildUserPrompt(String question, String currentSql, String promptContext) {
        StringBuilder builder = new StringBuilder();
        builder.append("Pergunta do usuario:\n").append(question).append("\n\n");
        if (currentSql != null && !currentSql.trim().isBlank()) {
            builder.append("SQL atual (opcional, como contexto):\n").append(currentSql.trim()).append("\n\n");
        }
        if (promptContext != null && !promptContext.trim().isBlank()) {
            builder.append("Contexto de metadata retrieval:\n").append(promptContext.trim()).append("\n\n");
        }
        builder.append("\nRetorne apenas o SQL final.");
        return builder.toString();
    }

    private String normalizeSqlOutput(String rawContent) {
        if (rawContent == null) {
            return "";
        }
        String normalized = rawContent.trim();
        if (normalized.startsWith("```")) {
            normalized = normalized.replaceAll("(?is)^```(?:sql)?\\s*", "");
            normalized = normalized.replaceAll("(?is)\\s*```$", "");
            normalized = normalized.trim();
        }
        normalized = normalized.replace("\r", "").trim();
        if (normalized.endsWith(";")) {
            return normalized;
        }
        if (normalized.toLowerCase(Locale.ROOT).startsWith("select ")) {
            return normalized + ";";
        }
        return normalized;
    }

    private String defaultText(String value, String fallback) {
        if (value == null || value.trim().isBlank()) {
            return fallback;
        }
        return value.trim();
    }
}

