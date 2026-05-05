package com.victor.sql_api.assistant.metadata_retrieval.application;

import com.victor.sql_api.assistant.metadata.model.context.MetadataContext;
import com.victor.sql_api.assistant.metadata.model.context.RelevantColumn;
import com.victor.sql_api.assistant.metadata.model.context.RelevantRelationship;
import com.victor.sql_api.assistant.metadata.model.context.RelevantTable;
import com.victor.sql_api.assistant.nl2sql.model.QueryUnderstanding;
import org.springframework.stereotype.Component;

import java.util.Locale;

@Component
public class PromptContextBuilder {

    public String build(String question, QueryUnderstanding understanding, MetadataContext metadataContext) {
        StringBuilder b = new StringBuilder();
        b.append("QUESTION:\n").append(orEmpty(question)).append("\n\n");

        b.append("QUERY_UNDERSTANDING:\n");
        b.append("intent: ").append(understanding == null ? "UNKNOWN" : understanding.intent()).append("\n");
        b.append("metrics: ").append(understanding == null ? "[]" : understanding.metrics()).append("\n");
        b.append("dimensions: ").append(understanding == null ? "[]" : understanding.dimensions()).append("\n");
        b.append("filters: ").append(understanding == null ? "[]" : understanding.filters()).append("\n\n");

        b.append("ALLOWED_TABLES:\n");
        for (RelevantTable t : metadataContext.tables()) {
            b.append("- ").append(t.schemaName()).append(".").append(t.tableName()).append(": ")
                    .append(orEmpty(t.businessDescription())).append("\n");
        }

        b.append("\nALLOWED_COLUMNS:\n");
        for (RelevantColumn c : metadataContext.columns()) {
            b.append("- ").append(c.schemaName()).append(".").append(c.tableName()).append(".").append(c.columnName())
                    .append(" (").append(defaultType(c.dataType())).append("): ")
                    .append(orEmpty(c.description()));
            if (c.semanticRole() != null && !c.semanticRole().isBlank()) {
                b.append(" | ").append(c.semanticRole());
            }
            b.append("\n");
        }

        b.append("\nRELATIONSHIPS:\n");
        for (RelevantRelationship r : metadataContext.relationships()) {
            b.append("- ").append(r.fromSchema()).append(".").append(r.fromTable()).append(".").append(r.fromColumn())
                    .append(" -> ")
                    .append(r.toSchema()).append(".").append(r.toTable()).append(".").append(r.toColumn())
                    .append("\n");
        }

        b.append("\nINSTRUCTIONS:\n");
        b.append("- Generate only SELECT SQL.\n");
        b.append("- Use only allowed tables and columns.\n");
        b.append("- Do not invent columns.\n");
        b.append("- Prefer explicit joins from RELATIONSHIPS.\n");

        return b.toString();
    }

    private String orEmpty(String value) {
        return value == null ? "" : value;
    }

    private String defaultType(String value) {
        return value == null || value.isBlank() ? "unknown" : value.toLowerCase(Locale.ROOT);
    }
}









