package com.victor.sql_api.assistant.metadata_retrieval.application;

import com.victor.sql_api.assistant.metadata_retrieval.model.RetrievalConstraints;
import com.victor.sql_api.assistant.metadata_retrieval.model.RetrievalRequest;
import com.victor.sql_api.assistant.metadata.infrastructure.MetadataCatalogGateway;
import com.victor.sql_api.assistant.metadata.model.context.*;
import com.victor.sql_api.assistant.nl2sql.model.QueryUnderstanding;
import com.victor.sql_api.shared.exception.BadRequestException;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.util.Span;
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
    private static final Set<String> CONTENT_POS_TAGS = Set.of("NOUN", "PROPN", "ADJ", "NUM", "VERB");
    private final MetadataCatalogGateway metadataCatalogGateway;
    private final POSTaggerME posTagger;
    private final SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;

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
        Set<String> questionTokens = extractSemanticTokens(request.question());
        List<ColumnMeta> allColumnsFromAllTables = fetchColumns(allTables);
        Map<String, List<ColumnMeta>> columnsByTable = allColumnsFromAllTables.stream()
                .collect(Collectors.groupingBy(c -> tableKey(c.schemaName, c.tableName), LinkedHashMap::new, Collectors.toList()));

        List<ScoredTable> selectedScoredTables = selectTables(
                allTables,
                columnsByTable,
                questionTokens,
                constraints.maxTables(),
                constraints.minTableScoreThreshold()
        );
        List<TableMeta> selectedTablesMeta = selectedScoredTables.stream().map(st -> st.table).toList();
        Map<String, Double> scoreByTableKey = selectedScoredTables.stream()
                .collect(Collectors.toMap(
                        st -> tableKey(st.table.schemaName, st.table.tableName),
                        st -> clampScore(st.finalScore),
                        (left, right) -> left,
                        LinkedHashMap::new
                ));

        Set<String> selectedTableKeys = selectedTablesMeta.stream()
                .map(table -> tableKey(table.schemaName, table.tableName))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        List<ColumnMeta> columnsInSelectedTables = allColumnsFromAllTables.stream()
                .filter(c -> selectedTableKeys.contains(tableKey(c.schemaName, c.tableName)))
                .toList();

        List<ScoredColumn> selectedScoredColumns = selectColumns(
                columnsInSelectedTables,
                questionTokens,
                constraints.maxColumnsPerTable(),
                constraints.maxTotalColumns(),
                constraints.minColumnScoreThreshold()
        );
        List<ColumnMeta> selectedColumnsMeta = selectedScoredColumns.stream().map(sc -> sc.column).toList();
        Map<String, Double> scoreByColumnKey = selectedScoredColumns.stream()
                .collect(Collectors.toMap(
                        sc -> columnKey(sc.column.schemaName, sc.column.tableName, sc.column.columnName),
                        sc -> clampScore(sc.score),
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
                        scoreByTableKey.getOrDefault(tableKey(table.schemaName, table.tableName), 0.0)
                ))
                .toList();

        List<RelevantColumn> relevantColumns = selectedScoredColumns.stream()
                .map(scored -> {
                    ColumnMeta column = scored.column;
                    double score = clampScore(scored.score);
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

        List<String> tableDetails = selectedScoredTables.stream()
                .map(scored -> {
                    String key = tableKey(scored.table.schemaName, scored.table.tableName);
                    return key
                            + " | score=" + format(clampScore(scored.finalScore))
                            + " | nlp_table=" + format(scored.baseTableScore)
                            + " | nlp_columns=" + format(scored.baseColumnsScore)
                            + " | reason=opennlp_similarity";
                })
                .toList();

        List<String> columnDetails = selectedColumnsMeta.stream()
                .map(column -> {
                    String key = columnKey(column.schemaName, column.tableName, column.columnName);
                    double score = scoreByColumnKey.getOrDefault(key, 0.0);
                    return key + " | score=" + format(score) + " | reason=opennlp_similarity";
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

    private List<ScoredColumn> selectColumns(
            List<ColumnMeta> allColumns,
            Set<String> questionTokens,
            int maxColumnsPerTable,
            int maxTotalColumns,
            double minColumnScoreThreshold
    ) {
        if (allColumns.isEmpty()) {
            return List.of();
        }

        List<ScoredColumn> ranked = allColumns.stream()
                .map(column -> new ScoredColumn(column, scoreColumn(column, questionTokens)))
                .sorted(Comparator.comparingDouble((ScoredColumn sc) -> sc.score).reversed())
                .toList();

        List<ScoredColumn> priority = ranked.stream()
                .filter(scored -> scored.score >= minColumnScoreThreshold)
                .toList();

        if (priority.isEmpty()) {
            priority = ranked;
        }

        int safePerTable = Math.max(0, maxColumnsPerTable);
        int safeTotal = Math.max(0, maxTotalColumns);
        Map<String, Integer> countByTable = new HashMap<>();
        List<ScoredColumn> result = new ArrayList<>();

        for (ScoredColumn scored : priority) {
            if (safeTotal > 0 && result.size() >= safeTotal) {
                break;
            }
            ColumnMeta column = scored.column;
            String tableKey = tableKey(column.schemaName, column.tableName);
            int tableCount = countByTable.getOrDefault(tableKey, 0);
            if (safePerTable > 0 && tableCount >= safePerTable) {
                continue;
            }
            result.add(scored);
            countByTable.put(tableKey, tableCount + 1);
        }

        return result;
    }

    private List<ScoredTable> selectTables(
            List<TableMeta> allTables,
            Map<String, List<ColumnMeta>> columnsByTable,
            Set<String> questionTokens,
            int maxTables,
            double minTableScoreThreshold
    ) {
        int safeMaxTables = Math.max(0, maxTables);
        if (safeMaxTables == 0 || allTables.isEmpty()) {
            return List.of();
        }

        List<ScoredTable> ranked = allTables.stream()
                .map(table -> scoreTable(table, columnsByTable, questionTokens))
                .sorted(Comparator.comparingDouble((ScoredTable st) -> st.finalScore).reversed())
                .toList();

        List<ScoredTable> priority = ranked.stream()
                .filter(scored -> scored.finalScore >= minTableScoreThreshold)
                .toList();

        if (priority.isEmpty()) {
            priority = ranked;
        } else {
            int minimumRecall = Math.min(2, safeMaxTables);
            if (priority.size() < minimumRecall) {
                LinkedHashMap<String, ScoredTable> merged = new LinkedHashMap<>();
                for (ScoredTable scored : priority) {
                    merged.put(tableKey(scored.table.schemaName, scored.table.tableName), scored);
                }
                for (ScoredTable scored : ranked) {
                    if (merged.size() >= minimumRecall) {
                        break;
                    }
                    merged.putIfAbsent(tableKey(scored.table.schemaName, scored.table.tableName), scored);
                }
                priority = new ArrayList<>(merged.values());
            }
        }
        return priority.stream().limit(safeMaxTables).toList();
    }

    private ScoredTable scoreTable(
            TableMeta table,
            Map<String, List<ColumnMeta>> columnsByTable,
            Set<String> questionTokens
    ) {
        String tableTokensText = table.tableName + " " + defaultString(table.tableDescription);
        double tableNlpScore = openNlpSimilarity(questionTokens, tableTokensText);
        List<ColumnMeta> tableColumns = columnsByTable.getOrDefault(tableKey(table.schemaName, table.tableName), List.of());
        double columnsNlpScore = tableColumns.stream()
                .mapToDouble(col -> {
                    String colText = col.columnName + " " + defaultString(col.columnComment) + " " + defaultString(col.semanticRole);
                    return openNlpSimilarity(questionTokens, colText);
                })
                .max()
                .orElse(0.0);
        double finalScore = Math.max(tableNlpScore, columnsNlpScore);
        return new ScoredTable(table, finalScore, tableNlpScore, columnsNlpScore);
    }

    private double scoreColumn(
            ColumnMeta column,
            Set<String> questionTokens
    ) {
        String columnTokensText = column.columnName + " " + defaultString(column.columnComment) + " " + defaultString(column.semanticRole);
        return openNlpSimilarity(questionTokens, columnTokensText);
    }

    private double cosineSimilarity(Set<String> left, Set<String> right) {
        if (left == null || right == null || left.isEmpty() || right.isEmpty()) {
            return 0.0;
        }
        int matches = 0;
        for (String token : left) {
            if (right.contains(token)) {
                matches++;
            }
        }
        return matches / (Math.sqrt(left.size()) * Math.sqrt(right.size()));
    }

    private double openNlpSimilarity(Set<String> questionSemanticTokens, String metadataText) {
        if (posTagger == null || questionSemanticTokens == null || questionSemanticTokens.isEmpty()) {
            return 0.0;
        }
        Set<String> metadataContentTokens = extractSemanticTokens(metadataText);
        return cosineSimilarity(questionSemanticTokens, metadataContentTokens);
    }

    private Set<String> filterContentTokensWithPos(List<String> tokens) {
        if (tokens == null || tokens.isEmpty() || posTagger == null) {
            return Set.of();
        }
        String[] tags = posTagger.tag(tokens.toArray(String[]::new));
        LinkedHashSet<String> filtered = new LinkedHashSet<>();
        for (int i = 0; i < tokens.size(); i++) {
            String tag = tags[i] == null ? "" : tags[i].toUpperCase(Locale.ROOT);
            if (containsAnyPosTag(tag)) {
                filtered.add(tokens.get(i));
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

    private List<String> tokenize(String normalized) {
        if (normalized == null || normalized.isBlank()) return List.of();
        Span[] spans = tokenizer.tokenizePos(normalized);
        List<String> tokens = new ArrayList<>(spans.length);
        for (Span span : spans) {
            String token = normalized.substring(span.getStart(), span.getEnd()).trim();
            if (!token.isBlank()) tokens.add(token);
        }
        return tokens;
    }

    private Set<String> extractSemanticTokens(String rawText) {
        if (rawText == null || rawText.isBlank()) {
            return Set.of();
        }
        String normalized = normalizeText(rawText);
        List<String> tokens = tokenize(normalized);
        return filterContentTokensWithPos(tokens);
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

    private record ScoredTable(
            TableMeta table,
            double finalScore,
            double baseTableScore,
            double baseColumnsScore
    ) {
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

    private record ScoredColumn(ColumnMeta column, double score) {
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








