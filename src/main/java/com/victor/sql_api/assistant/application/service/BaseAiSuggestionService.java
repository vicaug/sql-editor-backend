package com.victor.sql_api.assistant.application.service;

import com.victor.sql_api.assistant.application.model.AiAssistantResult;
import com.victor.sql_api.assistant.application.port.AiTextGenerator;
import com.victor.sql_api.assistant.context.application.router.MetadataContextProviderRouter;
import com.victor.sql_api.assistant.retrieval.model.application.RetrievalRequest;
import com.victor.sql_api.assistant.retrieval.model.domain.MetadataContext;
import com.victor.sql_api.assistant.retrieval.model.domain.RelevantColumn;
import com.victor.sql_api.assistant.retrieval.model.domain.RelevantRelationship;
import com.victor.sql_api.assistant.retrieval.model.domain.RelevantTable;
import com.victor.sql_api.assistant.nl2sql.application.service.SqlGuardService;
import com.victor.sql_api.assistant.nl2sql.domain.model.Nl2SqlContext;
import com.victor.sql_api.assistant.nl2sql.domain.model.SqlValidationResult;
import com.victor.sql_api.shared.exception.BadRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Locale;

abstract class BaseAiSuggestionService implements AiSuggestionService {
    /*
     * Source of truth for text-to-sql flow:
     * 1) Build NL2SQL context via metadata context provider router
     * 2) Call LLM text generator with strict SQL prompt
     * 3) Optionally validate generated SQL with SqlGuardService
     */
    private static final Logger log = LoggerFactory.getLogger(BaseAiSuggestionService.class);
    private static final String SYSTEM_PROMPT = """
            Você é um assistente de SQL para PostgreSQL.
            Regras obrigatórias:
            - Responda somente com SQL puro (sem markdown, sem explicação).
            - Gere apenas uma consulta SQL.
            - Crie apenas SELECT.
            - Priorize traser os descritores das tabelas, não somente os IDs.
            - Use apenas tabelas/colunas presentes no contexto de metadata.
            - Se houver caminhos de relacionamento, prefira joins coerentes por eles.
            - Se o pedido estiver ambíguo, escolha a interpretação mais provável com base no contexto.
            - Caso não hajam tabelas relevantes, retorne: "Não foi possível gerar SQL com base na solicitação e no contexto fornecidos."
            """;

    private final MetadataContextProviderRouter metadataContextRouterService;
    private final AiTextGenerator aiTextGenerator;
    private final SqlGuardService sqlGuardService;

    protected BaseAiSuggestionService(
            MetadataContextProviderRouter metadataContextRouterService,
            AiTextGenerator aiTextGenerator,
            SqlGuardService sqlGuardService
    ) {
        this.metadataContextRouterService = metadataContextRouterService;
        this.aiTextGenerator = aiTextGenerator;
        this.sqlGuardService = sqlGuardService;
    }

    @Override
    public AiAssistantResult suggest(
            String prompt,
            String currentSql,
            boolean enableSqlGuard,
            String metadataProvider,
            String queryUnderstandingEngine
    ) {
        String question = prompt == null ? "" : prompt.trim();
        if (question.isBlank()) {
            throw new BadRequestException("AI_PROMPT_EMPTY", "O prompt da pergunta em linguagem natural e obrigatorio.");
        }

        Nl2SqlContext nl2SqlContext;
        long metadataStartedAt = System.nanoTime();
        try {
            nl2SqlContext = metadataContextRouterService.buildContext(
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
        String userPrompt = buildUserPrompt(question, currentSql, metadata, nl2SqlContext.promptContext());
        long llmStartedAt = System.nanoTime();
        String rawContent = aiTextGenerator.generate(SYSTEM_PROMPT, userPrompt);
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

    private String buildUserPrompt(String question, String currentSql, MetadataContext metadata, String promptContext) {
        StringBuilder builder = new StringBuilder();
        builder.append("Pergunta do usuario:\n").append(question).append("\n\n");

        if (currentSql != null && !currentSql.trim().isBlank()) {
            builder.append("SQL atual (opcional, como contexto):\n").append(currentSql.trim()).append("\n\n");
        }
        if (promptContext != null && !promptContext.trim().isBlank()) {
            builder.append("Contexto AskDataLike:\n").append(promptContext.trim()).append("\n\n");
        }

        builder.append("Tabelas relevantes:\n");
        appendTables(builder, metadata.tables());

        builder.append("\nColunas relevantes:\n");
        appendColumns(builder, metadata.columns());

        builder.append("\nRelacionamentos relevantes:\n");
        appendRelationships(builder, metadata.relationships());

        builder.append("\nRetorne apenas o SQL final.");
        return builder.toString();
    }

    private void appendTables(StringBuilder builder, List<RelevantTable> tables) {
        if (tables == null || tables.isEmpty()) {
            builder.append("- (nenhuma tabela relevante encontrada)\n");
            return;
        }
        for (RelevantTable table : tables) {
            builder.append("- ").append(table.schemaName()).append(".").append(table.tableName());
            if (table.businessDescription() != null && !table.businessDescription().isBlank()) {
                builder.append(" | desc: ").append(table.businessDescription().trim());
            }
            builder.append("\n");
        }
    }

    private void appendColumns(StringBuilder builder, List<RelevantColumn> columns) {
        if (columns == null || columns.isEmpty()) {
            builder.append("- (nenhuma coluna relevante encontrada)\n");
            return;
        }
        for (RelevantColumn column : columns) {
            builder.append("- ")
                    .append(column.schemaName()).append(".")
                    .append(column.tableName()).append(".")
                    .append(column.columnName())
                    .append(" (").append(defaultText(column.dataType(), "unknown")).append(")");
            if (column.primaryKey()) {
                builder.append(" [PK]");
            }
            if (column.foreignKey()) {
                builder.append(" [FK]");
            }
            if (column.semanticRole() != null && !column.semanticRole().isBlank()) {
                builder.append(" | role: ").append(column.semanticRole().trim());
            }
            builder.append("\n");
        }
    }

    private void appendRelationships(StringBuilder builder, List<RelevantRelationship> relationships) {
        if (relationships == null || relationships.isEmpty()) {
            builder.append("- (nenhum relacionamento relevante encontrado)\n");
            return;
        }
        for (RelevantRelationship relationship : relationships) {
            builder.append("- ")
                    .append(relationship.fromSchema()).append(".")
                    .append(relationship.fromTable()).append(".")
                    .append(relationship.fromColumn())
                    .append(" -> ")
                    .append(relationship.toSchema()).append(".")
                    .append(relationship.toTable()).append(".")
                    .append(relationship.toColumn());
            if (relationship.relationshipType() != null && !relationship.relationshipType().isBlank()) {
                builder.append(" | ").append(relationship.relationshipType().trim());
            }
            builder.append("\n");
        }
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



