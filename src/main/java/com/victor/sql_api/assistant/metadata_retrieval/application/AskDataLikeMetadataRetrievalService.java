package com.victor.sql_api.assistant.metadata_retrieval.application;

import com.victor.sql_api.assistant.metadata_retrieval.model.RetrievalConstraints;
import com.victor.sql_api.assistant.metadata_retrieval.model.RetrievalRequest;
import com.victor.sql_api.assistant.metadata.infrastructure.MetadataCatalogGateway;
import com.victor.sql_api.assistant.metadata.model.context.*;
import com.victor.sql_api.assistant.nl2sql.model.QueryUnderstanding;
import com.victor.sql_api.shared.exception.BadRequestException;
import opennlp.tools.postag.POSTaggerME;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger log = LoggerFactory.getLogger(AskDataLikeMetadataRetrievalService.class);

    private static final Set<String> STOPWORDS = Set.of(
            "a", "o", "os", "as", "de", "do", "da", "dos", "das", "e", "em", "para", "por", "com",
            "um", "uma", "meu", "minha", "gere", "trazer", "calcule", "tambem", "quero", "que", "mostre"
    );
    private static final Set<String> CONTENT_POS_TAGS = Set.of("NOUN", "PROPN", "ADJ", "NUM", "VERB");

    private final MetadataCatalogGateway metadataCatalogGateway;
    private final POSTaggerME posTagger;

    public AskDataLikeMetadataRetrievalService(
            MetadataCatalogGateway metadataCatalogGateway,
            OpenNlpPosTaggerProvider posTaggerProvider
    ) {
        this.metadataCatalogGateway = metadataCatalogGateway;
        this.posTagger = posTaggerProvider.getPosTagger();
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

        List<TableMeta> selectedTablesMeta = selectTables(
                allTables,
                columnsByTable,
                questionTokens,
                constraints.maxTables()
        );

        Set<String> selectedTableKeys = selectedTablesMeta.stream()
                .map(table -> tableKey(table.schemaName, table.tableName))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        List<ColumnMeta> columnsInSelectedTables = allColumnsFromAllTables.stream()
                .filter(c -> selectedTableKeys.contains(tableKey(c.schemaName, c.tableName)))
                .toList();

        List<ColumnMeta> selectedColumnsMeta = selectColumns(
                columnsInSelectedTables,
                questionTokens,
                understanding,
                constraints.maxColumnsPerTable(),
                constraints.maxTotalColumns()
        );

        Map<String, Double> scoreByColumnKey = selectedColumnsMeta.stream()
                .collect(Collectors.toMap(
                        c -> columnKey(c.schemaName, c.tableName, c.columnName),
                        c -> clampScore(scoreColumn(c, questionTokens, understanding)),
                        (left, right) -> left,
                        LinkedHashMap::new
                ));

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
                    String key = columnKey(column.schemaName, column.tableName, column.columnName);
                    double score = scoreByColumnKey.getOrDefault(key, 0.0);
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

        List<String> tableDetails = selectedTablesMeta.stream()
                .map(table -> table.schemaName + "." + table.tableName
                        + " | reason=semantic_match")
                .toList();

        List<String> columnDetails = selectedColumnsMeta.stream()
                .map(column -> {
                    String key = columnKey(column.schemaName, column.tableName, column.columnName);
                    return key + " | score=" + format(scoreByColumnKey.getOrDefault(key, 0.0)) + " | reason=structural_context_column";
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
                columnsInSelectedTables.size(),
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
        List<Map<String, Object>> rows = metadataCatalogGateway.fetchActiveTables();

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

        List<Map<String, Object>> rows = metadataCatalogGateway.fetchColumnsByTableFilter(tableFilter, params.toArray());
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

        List<Map<String, Object>> rows = metadataCatalogGateway.fetchRelationshipsByTableFilters(sourceFilter, targetFilter, params.toArray());
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
            QueryUnderstanding understanding,
            int maxColumnsPerTable,
            int maxTotalColumns
    ) {
        if (allColumns.isEmpty()) {
            return List.of();
        }

        List<ColumnMeta> priority = allColumns.stream()
                .sorted(Comparator.comparingDouble((ColumnMeta c) -> scoreColumn(c, questionTokens, understanding)).reversed())
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
            int maxTables
    ) {
        int safeMaxTables = Math.max(0, maxTables);
        if (safeMaxTables == 0 || allTables.isEmpty()) {
            return List.of();
        }

        return allTables.stream()
                .sorted(Comparator.comparingDouble((TableMeta t) -> scoreTable(t, columnsByTable, questionTokens)).reversed())
                .limit(safeMaxTables)
                .toList();
    }

    private double scoreTable(
            TableMeta table,
            Map<String, List<ColumnMeta>> columnsByTable,
            Set<String> questionTokens
    ) {
        String tableTokensText = normalizeText(table.tableName + " " + defaultString(table.tableDescription));
        Set<String> tableTokens = tokenize(tableTokensText);
        double semanticScore = overlapScore(questionTokens, tableTokens);
        double nlpScore = openNlpSimilarity(questionTokens, tableTokensText);
        double blendedTableScore = blendedSimilarity(semanticScore, nlpScore);
        List<ColumnMeta> tableColumns = columnsByTable.getOrDefault(tableKey(table.schemaName, table.tableName), List.of());
        double columnsSemanticScore = tableColumns.stream()
                .mapToDouble(col -> {
                    String colText = normalizeText(col.columnName + " " + defaultString(col.columnComment) + " " + defaultString(col.semanticRole));
                    double colOverlapScore = overlapScore(questionTokens, tokenize(colText));
                    return blendedSimilarity(colOverlapScore, openNlpSimilarity(questionTokens, colText));
                })
                .max()
                .orElse(0.0);
        return (blendedTableScore * 0.55) + (columnsSemanticScore * 0.45);
    }

    private double scoreColumn(
            ColumnMeta column,
            Set<String> questionTokens,
            QueryUnderstanding understanding
    ) {
        String columnTokensText = normalizeText(column.columnName + " " + defaultString(column.columnComment) + " " + defaultString(column.semanticRole));
        Set<String> columnTokens = tokenize(columnTokensText);
        double semanticScore = overlapScore(questionTokens, columnTokens);
        double nlpScore = openNlpSimilarity(questionTokens, columnTokensText);
        double blendedScore = blendedSimilarity(semanticScore, nlpScore);
        double structuralBoost = (column.primaryKey || column.foreignKey) ? 0.10 : 0.0;
        double roleBoost = matchesUnderstandingRole(column.semanticRole, understanding) ? 0.10 : 0.0;
        return blendedScore + structuralBoost + roleBoost;
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

    private double blendedSimilarity(double overlapScore, double nlpScore) {
        if (nlpScore <= 0.0) {
            return overlapScore;
        }
        return (overlapScore * 0.60) + (nlpScore * 0.40);
    }

    private double openNlpSimilarity(Set<String> normalizedQuestionTokens, String metadataText) {
        if (posTagger == null || normalizedQuestionTokens == null || normalizedQuestionTokens.isEmpty()) {
            return 0.0;
        }
        Set<String> questionContentTokens = filterContentTokensWithPos(normalizedQuestionTokens);
        Set<String> metadataContentTokens = filterContentTokensWithPos(tokenize(metadataText));
        return overlapScore(questionContentTokens, metadataContentTokens);
    }

    private Set<String> filterContentTokensWithPos(Set<String> tokens) {
        if (tokens == null || tokens.isEmpty() || posTagger == null) {
            return Set.of();
        }
        List<String> tokenList = new ArrayList<>(tokens);
        String[] tags = posTagger.tag(tokenList.toArray(String[]::new));
        LinkedHashSet<String> filtered = new LinkedHashSet<>();
        for (int i = 0; i < tokenList.size(); i++) {
            String tag = tags[i] == null ? "" : tags[i].toUpperCase(Locale.ROOT);
            if (containsAnyPosTag(tag)) {
                filtered.add(tokenList.get(i));
            }
        }
        return filtered;
    }

    private boolean containsAnyPosTag(String tag) {
        for (String contentTag : CONTENT_POS_TAGS) {
            if (tag.contains(contentTag)) {
                return true;
            }
        }
        return false;
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

    private double clampScore(double value) {
        return Math.max(0.0, Math.min(1.0, value));
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








