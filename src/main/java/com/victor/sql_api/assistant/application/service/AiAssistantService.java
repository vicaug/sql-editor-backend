package com.victor.sql_api.assistant.application.service;

import com.victor.sql_api.assistant.application.model.AiAssistantResult;
import com.victor.sql_api.assistant.metadata.application.model.RetrievalRequest;
import com.victor.sql_api.assistant.metadata.application.service.MetadataRetrievalService;
import com.victor.sql_api.assistant.metadata.domain.model.MetadataContext;
import com.victor.sql_api.assistant.metadata.domain.model.RelevantColumn;
import com.victor.sql_api.assistant.metadata.domain.model.RelevantRelationship;
import com.victor.sql_api.assistant.metadata.domain.model.RelevantTable;
import com.victor.sql_api.shared.exception.BadRequestException;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Locale;

@Service
public class AiAssistantService {
    private static final String SYSTEM_PROMPT = """
            Você é um assistente de SQL para PostgreSQL.
            Regras obrigatórias:
            - Responda somente com SQL puro (sem markdown, sem explicação).
            - Gere apenas uma consulta SQL.
            - Priorize SELECT.
            - Use apenas tabelas/colunas presentes no contexto de metadata.
            - Se houver caminhos de relacionamento, prefira joins coerentes por eles.
            - Se o pedido estiver ambíguo, escolha a interpretação mais provável com base no contexto.
            """;

    private final MetadataRetrievalService metadataRetrievalService;
    private final ChatClient chatClient;

    public AiAssistantService(MetadataRetrievalService metadataRetrievalService, ChatClient.Builder chatClientBuilder) {
        this.metadataRetrievalService = metadataRetrievalService;
        this.chatClient = chatClientBuilder.build();
    }

    public AiAssistantResult suggest(String prompt, String currentSql) {
        String question = prompt == null ? "" : prompt.trim();
        if (question.isBlank()) {
            throw new BadRequestException("AI_PROMPT_EMPTY", "O prompt da pergunta em linguagem natural é obrigatório.");
        }

        MetadataContext metadata = metadataRetrievalService.retrieve(new RetrievalRequest(question, null));
        String userPrompt = buildUserPrompt(question, currentSql, metadata);

        String rawContent = chatClient.prompt()
                .system(SYSTEM_PROMPT)
                .user(userPrompt)
                .call()
                .content();
        String sql = normalizeSqlOutput(rawContent);
        if (sql.isBlank()) {
            throw new BadRequestException("AI_SQL_EMPTY", "A IA não retornou SQL válido para a solicitação.");
        }

        return new AiAssistantResult(sql, Instant.now(), metadata);
    }

    private String buildUserPrompt(String question, String currentSql, MetadataContext metadata) {
        StringBuilder builder = new StringBuilder();
        builder.append("Pergunta do usuário:\n")
                .append(question)
                .append("\n\n");

        if (currentSql != null && !currentSql.trim().isBlank()) {
            builder.append("SQL atual (opcional, como contexto):\n")
                    .append(currentSql.trim())
                    .append("\n\n");
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
            builder.append("- ")
                    .append(table.schemaName())
                    .append(".")
                    .append(table.tableName());
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
                    .append(column.schemaName())
                    .append(".")
                    .append(column.tableName())
                    .append(".")
                    .append(column.columnName())
                    .append(" (")
                    .append(defaultText(column.dataType(), "unknown"))
                    .append(")");
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
