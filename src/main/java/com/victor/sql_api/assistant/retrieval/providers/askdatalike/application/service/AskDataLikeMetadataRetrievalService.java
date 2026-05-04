package com.victor.sql_api.assistant.retrieval.providers.askdatalike.application.service;

import com.victor.sql_api.assistant.retrieval.model.application.RetrievalConstraints;
import com.victor.sql_api.assistant.retrieval.model.application.RetrievalRequest;
import com.victor.sql_api.assistant.retrieval.model.domain.*;
import com.victor.sql_api.assistant.nl2sql.domain.model.QueryUnderstanding;
import com.victor.sql_api.shared.exception.BadRequestException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.text.Normalizer;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class AskDataLikeMetadataRetrievalService {
    /*
     * Source of truth for retrieval:
     * this service queries eqt_metadata tables and ranks relevant tables/columns/relationships.
     */
    private static final Set<String> STOPWORDS = Set.of(
            "a", "o", "os", "as", "de", "do", "da", "dos", "das", "e", "em", "para", "por", "com",
            "um", "uma", "meu", "minha", "gere", "trazer", "calcule", "tambem", "quero", "que", "mostre"
    );

    private final JdbcTemplate jdbcTemplate;

    public AskDataLikeMetadataRetrievalService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public MetadataContext retrieve(RetrievalRequest request) {
        return retrieve(request, null, "unknown", false, null);
    }

    public MetadataContext retrieve(RetrievalRequest request, QueryUnderstanding understanding) {
        return retrieve(request, understanding, "unknown", false, null);
    }

    public MetadataContext retrieve(
            RetrievalRequest request,
            QueryUnderstanding understanding,
            String queryUnderstandingEngine,
            boolean queryUnderstandingFallbackApplied,
            String queryUnderstandingFallbackReason
    ) {
        if (request == null || request.question() == null || request.question().trim().isEmpty()) {
            throw new BadRequestException("METADATA_QUESTION_EMPTY", "A pergunta para retrieval de metadata nao pode ser vazia.");
        }

        RetrievalConstraints constraints = request.effectiveConstraints();

        List<TableMeta> allTables = fetchTables();
        Set<String> questionTokens = tokenize(normalizeText(request.question()));
        List<ColumnMeta> allColumnsFromAllTables = fetchColumns(allTables);
        Map<String, List<ColumnMeta>> columnsByTable = allColumnsFromAllTables.stream()
                .collect(Collectors.groupingBy(c -> tableKey(c.schemaName, c.tableName), LinkedHashMap::new, Collectors.toList()));
        Set<String> forcedTableKeys = Set.of();

        List<TableMeta> selectedTablesMeta = selectTables(
                allTables,
                columnsByTable,
                questionTokens,
                forcedTableKeys,
                constraints.maxTables()
        );

        List<ColumnMeta> allColumns = allColumnsFromAllTables.stream()
                .filter(c -> selectedTablesMeta.stream().anyMatch(t -> tableKey(t.schemaName, t.tableName).equals(tableKey(c.schemaName, c.tableName))))
                .toList();
        Set<String> forcedColumnKeys = Set.of();

        List<ColumnMeta> selectedColumnsMeta = selectColumns(
                allColumns,
                questionTokens,
                forcedColumnKeys,
                understanding,
                constraints.maxColumnsPerTable(),
                constraints.maxTotalColumns()
        );

        List<RelationshipMeta> allRelationships = fetchRelationships(selectedTablesMeta);
        List<RelevantRelationship> selectedRelationships = selectRelationships(
                allRelationships,
                selectedColumnsMeta,
                constraints.maxRelationships()
        );

        List<RelevantTable> relevantTables = selectedTablesMeta.stream()
                .map(table -> new RelevantTable(
                        table.schemaName,
                        table.tableName,
                        table.tableDescription,
                        1.0
                ))
                .toList();

        List<RelevantColumn> relevantColumns = selectedColumnsMeta.stream()
                .map(column -> {
                    double score = forcedColumnKeys.contains(columnKey(column.schemaName, column.tableName, column.columnName))
                            ? 1.0
                            : 0.75;
                    return new RelevantColumn(
                            column.schemaName,
                            column.tableName,
                            column.columnName,
                            column.dataType,
                            column.semanticRole,
                            column.columnComment,
                            column.primaryKey,
                            column.foreignKey,
                            score
                    );
                })
                .toList();

        List<RetrievalHint> hints = new ArrayList<>();
        if (!selectedRelationships.isEmpty()) {
            hints.add(new RetrievalHint("RELATIONSHIP_HINT", "JOIN_PATH_AVAILABLE", 0.90));
        }

        boolean usedBusinessTermTables = !forcedTableKeys.isEmpty();
        List<String> tableDetails = selectedTablesMeta.stream()
                .map(table -> table.schemaName + "." + table.tableName
                        + " | reason=" + (usedBusinessTermTables ? "semantic_match+business_term_boost" : "semantic_match"))
                .toList();

        List<String> columnDetails = selectedColumnsMeta.stream()
                .map(column -> {
                    String key = columnKey(column.schemaName, column.tableName, column.columnName);
                    String reason = forcedColumnKeys.contains(key) ? "business_term_target_column" : "structural_context_column";
                    return key + " | reason=" + reason;
                })
                .toList();

        List<String> relationshipDetails = selectedRelationships.stream()
                .map(rel -> rel.fromSchema() + "." + rel.fromTable() + "." + rel.fromColumn()
                        + " -> " + rel.toSchema() + "." + rel.toTable() + "." + rel.toColumn()
                        + " | conf=" + format(rel.confidence()))
                .toList();

        RetrievalDiagnostics diagnostics = new RetrievalDiagnostics(
                questionTokens,
                queryUnderstandingEngine == null ? "unknown" : queryUnderstandingEngine,
                understanding == null ? 0.0 : understanding.confidence(),
                queryUnderstandingFallbackApplied,
                queryUnderstandingFallbackReason,
                allTables.size(),
                allColumns.size(),
                allRelationships.size(),
                relevantTables.size(),
                relevantColumns.size(),
                selectedRelationships.size(),
                tableDetails,
                columnDetails,
                relationshipDetails
        );

        return new MetadataContext(
                request.question(),
                relevantTables,
                relevantColumns,
                selectedRelationships,
                hints,
                diagnostics
        );
    }

    private List<TableMeta> fetchTables() {
        List<Map<String, Object>> rows = jdbcTemplate.queryForList("""
                select
                    t.table_schema,
                    t.table_name,
                    t.table_comment,
                    t.table_description_llm,
                    t.is_active
                from eqt_metadata.md_table t
                where coalesce(t.is_active, true) = true
                """);

        List<TableMeta> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            String schema = getString(row, "table_schema", "schema_name", "schema");
            String table = getString(row, "table_name", "name");
            if (schema == null || table == null) {
                continue;
            }
            String description = joinText(
                    getString(row, "table_comment", "comment"),
                    getString(row, "table_description_llm", "description_llm")
            );
            result.add(new TableMeta(schema, table, description));
        }
        return result;
    }

    private List<ColumnMeta> fetchColumns(List<TableMeta> selectedTables) {
        if (selectedTables == null || selectedTables.isEmpty()) {
            return List.of();
        }

        List<Object> params = new ArrayList<>();
        String tableFilter = buildTableFilter("t.table_schema", "t.table_name", selectedTables, params);

        String sql = """
                select
                    t.table_schema,
                    t.table_name,
                    c.column_name,
                    c.data_type,
                    c.column_comment,
                    c.semantic_role,
                    c.is_primary_key,
                    c.is_foreign_key
                from eqt_metadata.md_column c
                inner join eqt_metadata.md_table t on t.id_md_table = c.id_md_table
                where coalesce(t.is_active, true) = true
                  and (%s)
                """.formatted(tableFilter);

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, params.toArray());
        List<ColumnMeta> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            String schema = getString(row, "table_schema", "schema_name", "schema");
            String table = getString(row, "table_name");
            String column = getString(row, "column_name", "name");
            if (schema == null || table == null || column == null) {
                continue;
            }
            result.add(new ColumnMeta(
                    schema,
                    table,
                    column,
                    getString(row, "data_type", "column_type", "type"),
                    getString(row, "column_comment", "comment"),
                    getString(row, "semantic_role", "role"),
                    getBoolean(row, "is_primary_key", "is_pk", "primary_key", "pk"),
                    getBoolean(row, "is_foreign_key", "is_fk", "foreign_key", "fk")
            ));
        }
        return result;
    }

    private List<RelationshipMeta> fetchRelationships(List<TableMeta> selectedTables) {
        if (selectedTables == null || selectedTables.isEmpty()) {
            return List.of();
        }

        List<Object> params = new ArrayList<>();
        String sourceFilter = buildTableFilter("source_table_schema", "source_table_name", selectedTables, params);
        String targetFilter = buildTableFilter("target_table_schema", "target_table_name", selectedTables, params);

        String sql = """
                select
                    source_table_schema,
                    source_table_name,
                    source_column_name,
                    target_table_schema,
                    target_table_name,
                    target_column_name,
                    relationship_type
                from eqt_metadata.md_relationship
                where (%s)
                  and (%s)
                """.formatted(sourceFilter, targetFilter);

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, params.toArray());
        List<RelationshipMeta> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            String fromSchema = getString(row, "source_table_schema", "from_schema");
            String fromTable = getString(row, "source_table_name", "from_table");
            String fromColumn = getString(row, "source_column_name", "from_column");
            String toSchema = getString(row, "target_table_schema", "to_schema");
            String toTable = getString(row, "target_table_name", "to_table");
            String toColumn = getString(row, "target_column_name", "to_column");
            if (fromSchema == null || fromTable == null || fromColumn == null || toSchema == null || toTable == null || toColumn == null) {
                continue;
            }
            result.add(new RelationshipMeta(fromSchema, fromTable, fromColumn, toSchema, toTable, toColumn, getString(row, "relationship_type")));
        }
        return result;
    }

    private List<ColumnMeta> selectColumns(
            List<ColumnMeta> allColumns,
            Set<String> questionTokens,
            Set<String> forcedColumnKeys,
            QueryUnderstanding understanding,
            int maxColumnsPerTable,
            int maxTotalColumns
    ) {
        if (allColumns.isEmpty()) {
            return List.of();
        }

        List<ColumnMeta> priority = allColumns.stream()
                .sorted(Comparator.comparingDouble((ColumnMeta c) -> scoreColumn(c, questionTokens, forcedColumnKeys, understanding)).reversed())
                .toList();

        int safePerTable = Math.max(0, maxColumnsPerTable);
        int safeTotal = Math.max(0, maxTotalColumns);
        Map<String, Integer> countByTable = new HashMap<>();
        List<ColumnMeta> result = new ArrayList<>();

        for (ColumnMeta column : priority) {
            if (safeTotal > 0 && result.size() >= safeTotal) {
                break;
            }
            String tableKey = tableKey(column.schemaName, column.tableName);
            int tableCount = countByTable.getOrDefault(tableKey, 0);
            if (safePerTable > 0 && tableCount >= safePerTable) {
                continue;
            }
            result.add(column);
            countByTable.put(tableKey, tableCount + 1);
        }

        return result;
    }

    private List<TableMeta> selectTables(
            List<TableMeta> allTables,
            Map<String, List<ColumnMeta>> columnsByTable,
            Set<String> questionTokens,
            Set<String> forcedTableKeys,
            int maxTables
    ) {
        int safeMaxTables = Math.max(0, maxTables);
        if (safeMaxTables == 0 || allTables.isEmpty()) {
            return List.of();
        }

        return allTables.stream()
                .sorted(Comparator.comparingDouble((TableMeta t) -> scoreTable(t, columnsByTable, questionTokens, forcedTableKeys)).reversed())
                .limit(safeMaxTables)
                .toList();
    }

    private double scoreTable(
            TableMeta table,
            Map<String, List<ColumnMeta>> columnsByTable,
            Set<String> questionTokens,
            Set<String> forcedTableKeys
    ) {
        String tableTokensText = normalizeText(table.tableName + " " + defaultString(table.tableDescription));
        Set<String> tableTokens = tokenize(tableTokensText);
        double semanticScore = overlapScore(questionTokens, tableTokens);
        List<ColumnMeta> tableColumns = columnsByTable.getOrDefault(tableKey(table.schemaName, table.tableName), List.of());
        double columnsSemanticScore = tableColumns.stream()
                .mapToDouble(col -> overlapScore(
                        questionTokens,
                        tokenize(normalizeText(col.columnName + " " + defaultString(col.columnComment) + " " + defaultString(col.semanticRole)))
                ))
                .max()
                .orElse(0.0);
        double btBoost = forcedTableKeys.contains(tableKey(table.schemaName, table.tableName)) ? 0.35 : 0.0;
        return (semanticScore * 0.55) + (columnsSemanticScore * 0.45) + btBoost;
    }

    private double scoreColumn(
            ColumnMeta column,
            Set<String> questionTokens,
            Set<String> forcedColumnKeys,
            QueryUnderstanding understanding
    ) {
        String columnTokensText = normalizeText(column.columnName + " " + defaultString(column.columnComment) + " " + defaultString(column.semanticRole));
        Set<String> columnTokens = tokenize(columnTokensText);
        double semanticScore = overlapScore(questionTokens, columnTokens);
        double btBoost = forcedColumnKeys.contains(columnKey(column.schemaName, column.tableName, column.columnName)) ? 0.30 : 0.0;
        double structuralBoost = (column.primaryKey || column.foreignKey) ? 0.10 : 0.0;
        double roleBoost = matchesUnderstandingRole(column.semanticRole, understanding) ? 0.10 : 0.0;
        return semanticScore + btBoost + structuralBoost + roleBoost;
    }

    private double overlapScore(Set<String> left, Set<String> right) {
        if (left == null || right == null || left.isEmpty() || right.isEmpty()) {
            return 0.0;
        }
        int matches = 0;
        for (String token : left) {
            if (right.contains(token)) {
                matches++;
            }
        }
        return (double) matches / (double) left.size();
    }

    private boolean matchesUnderstandingRole(String semanticRole, QueryUnderstanding understanding) {
        if (understanding == null || semanticRole == null || semanticRole.isBlank()) {
            return false;
        }

        String role = semanticRole.toUpperCase(Locale.ROOT);
        if (understanding.requiresAggregation()) {
            if (role.contains("METRIC") || role.contains("MEASURE") || role.contains("AMOUNT") || role.contains("VALUE")) {
                return true;
            }
        }

        if (understanding.dimensions() != null && !understanding.dimensions().isEmpty()) {
            if (role.contains("DIMENSION") || role.contains("ATTRIBUTE") || role.contains("DATE") || role.contains("TIME")) {
                return true;
            }
        }

        return false;
    }

    private List<RelevantRelationship> selectRelationships(
            List<RelationshipMeta> relationships,
            List<ColumnMeta> selectedColumns,
            int maxRelationships
    ) {
        if (relationships.isEmpty()) {
            return List.of();
        }

        Set<String> selectedColumnKeys = selectedColumns.stream()
                .map(column -> columnKey(column.schemaName, column.tableName, column.columnName))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        List<RelevantRelationship> prioritized = new ArrayList<>();
        for (RelationshipMeta rel : relationships) {
            String from = columnKey(rel.fromSchema, rel.fromTable, rel.fromColumn);
            String to = columnKey(rel.toSchema, rel.toTable, rel.toColumn);
            double confidence = (selectedColumnKeys.contains(from) || selectedColumnKeys.contains(to)) ? 0.90 : 0.75;
            prioritized.add(new RelevantRelationship(
                    rel.fromSchema,
                    rel.fromTable,
                    rel.fromColumn,
                    rel.toSchema,
                    rel.toTable,
                    rel.toColumn,
                    rel.relationshipType,
                    confidence
            ));
        }

        int safeMax = Math.max(0, maxRelationships);
        if (safeMax == 0) {
            return List.of();
        }

        return prioritized.stream().limit(safeMax).toList();
    }

    private String buildTableFilter(String schemaColumn, String tableColumn, List<TableMeta> tables, List<Object> params) {
        StringJoiner joiner = new StringJoiner(" or ", "(", ")");
        for (TableMeta table : tables) {
            joiner.add("(" + schemaColumn + " = ? and " + tableColumn + " = ?)");
            params.add(table.schemaName);
            params.add(table.tableName);
        }
        return joiner.toString();
    }

    private String tableKey(String schema, String table) {
        return (schema + "." + table).toLowerCase(Locale.ROOT);
    }

    private String columnKey(String schema, String table, String column) {
        return (schema + "." + table + "." + column).toLowerCase(Locale.ROOT);
    }

    private String joinText(String first, String second) {
        String a = first == null ? "" : first.trim();
        String b = second == null ? "" : second.trim();
        if (a.isBlank() && b.isBlank()) {
            return "";
        }
        if (a.isBlank()) {
            return b;
        }
        if (b.isBlank()) {
            return a;
        }
        return a + " | " + b;
    }

    private String normalizeText(String value) {
        if (value == null) {
            return "";
        }
        String noAccent = Normalizer.normalize(value.toLowerCase(Locale.ROOT), Normalizer.Form.NFD)
                .replaceAll("\\p{M}", "");
        return noAccent.replaceAll("[^a-z0-9 ]", " ").replaceAll("\\s+", " ").trim();
    }

    private Set<String> tokenize(String normalized) {
        if (normalized == null || normalized.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(normalized.split(" "))
                .filter(token -> !token.isBlank())
                .filter(token -> token.length() > 1)
                .filter(token -> !STOPWORDS.contains(token))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private String defaultString(String value) {
        return value == null ? "" : value;
    }

    private String getString(Map<String, Object> row, String... keys) {
        for (String key : keys) {
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                if (!entry.getKey().equalsIgnoreCase(key) || entry.getValue() == null) {
                    continue;
                }
                String value = String.valueOf(entry.getValue()).trim();
                if (!value.isBlank()) {
                    return value;
                }
            }
        }
        return null;
    }

    private boolean getBoolean(Map<String, Object> row, String... keys) {
        for (String key : keys) {
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                if (!entry.getKey().equalsIgnoreCase(key) || entry.getValue() == null) {
                    continue;
                }
                Object value = entry.getValue();
                if (value instanceof Boolean boolValue) {
                    return boolValue;
                }
                if (value instanceof Number numberValue) {
                    return numberValue.intValue() != 0;
                }
                String text = String.valueOf(value).trim().toLowerCase(Locale.ROOT);
                if (text.equals("true") || text.equals("t") || text.equals("1") || text.equals("yes") || text.equals("y")) {
                    return true;
                }
                if (text.equals("false") || text.equals("f") || text.equals("0") || text.equals("no") || text.equals("n")) {
                    return false;
                }
            }
        }
        return false;
    }

    private String format(double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private record TableMeta(String schemaName, String tableName, String tableDescription) {
    }

    private record ColumnMeta(
            String schemaName,
            String tableName,
            String columnName,
            String dataType,
            String columnComment,
            String semanticRole,
            boolean primaryKey,
            boolean foreignKey
    ) {
    }

    private record RelationshipMeta(
            String fromSchema,
            String fromTable,
            String fromColumn,
            String toSchema,
            String toTable,
            String toColumn,
            String relationshipType
    ) {
    }
}



