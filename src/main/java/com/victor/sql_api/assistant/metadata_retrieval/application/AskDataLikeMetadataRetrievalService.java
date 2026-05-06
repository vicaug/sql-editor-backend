package com.victor.sql_api.assistant.metadata_retrieval.application;

import com.victor.sql_api.assistant.metadata_retrieval.model.RetrievalRequest;
import com.victor.sql_api.assistant.metadata.infrastructure.MetadataCatalogGateway;
import com.victor.sql_api.assistant.metadata.model.context.*;
import com.victor.sql_api.assistant.nl2sql.model.QueryUnderstanding;
import com.victor.sql_api.shared.exception.BadRequestException;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.util.Sequence;
import opennlp.tools.util.Span;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class AskDataLikeMetadataRetrievalService {
    /*
     * Source of truth for retrieval:
     * this service queries eqt_metadata tables and ranks relevant tables/columns/relationships.
     */
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

        List<TableMeta> allTables = fetchTables();
        TokenProfile questionProfile = buildTokenProfile(request.question());
        Set<String> questionTokens = questionProfile.tokens();
        List<ColumnMeta> allColumnsFromAllTables = fetchColumns(allTables);
        Map<String, List<ColumnMeta>> columnsByTable = allColumnsFromAllTables.stream()
                .collect(Collectors.groupingBy(c -> tableKey(c.schemaName, c.tableName), LinkedHashMap::new, Collectors.toList()));

        List<ScoredColumn> selectedScoredColumns = selectColumns(
                allColumnsFromAllTables,
                questionProfile
        );
        List<ColumnMeta> selectedColumnsMeta = selectedScoredColumns.stream().map(sc -> sc.column).toList();
        Map<String, Double> scoreByColumnKey = selectedScoredColumns.stream()
                .collect(Collectors.toMap(
                        sc -> columnKey(sc.column.schemaName, sc.column.tableName, sc.column.columnName),
                        sc -> sc.score,
                        (left, right) -> left,
                        LinkedHashMap::new
                ));

        Set<String> selectedTableKeys = selectedColumnsMeta.stream()
                .map(column -> tableKey(column.schemaName, column.tableName))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        List<ScoredTable> selectedScoredTables = selectTables(
                allTables,
                columnsByTable,
                questionProfile
        ).stream()
                .filter(scored -> selectedTableKeys.contains(tableKey(scored.table.schemaName, scored.table.tableName)))
                .toList();

        List<TableMeta> selectedTablesMeta = selectedScoredTables.stream().map(st -> st.table).toList();
        Map<String, Double> scoreByTableKey = selectedScoredTables.stream()
                .collect(Collectors.toMap(
                        st -> tableKey(st.table.schemaName, st.table.tableName),
                        st -> st.finalScore,
                        (left, right) -> left,
                        LinkedHashMap::new
                ));

        List<RelationshipMeta> allRelationships = fetchRelationships(selectedTablesMeta);
        List<RelevantRelationship> selectedRelationships = selectRelationships(
                allRelationships,
                selectedColumnsMeta,
                questionProfile
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
                    double score = scored.score;
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
            hints.add(new RetrievalHint("RELATIONSHIP_HINT", "JOIN_PATH_AVAILABLE", selectedRelationships.get(0).confidence()));
        }

        List<String> tableDetails = selectedScoredTables.stream()
                .map(scored -> {
                    String key = tableKey(scored.table.schemaName, scored.table.tableName);
                    return key
                            + " | score=" + format(scored.finalScore)
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
                allColumnsFromAllTables.size(),
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
            TokenProfile questionProfile
    ) {
        if (allColumns.isEmpty()) {
            return List.of();
        }

        List<ScoredColumn> ranked = allColumns.stream()
                .map(column -> new ScoredColumn(column, scoreColumn(column, questionProfile)))
                .sorted(Comparator.comparingDouble((ScoredColumn sc) -> sc.score).reversed())
                .toList();

        return ranked;
    }

    private List<ScoredTable> selectTables(
            List<TableMeta> allTables,
            Map<String, List<ColumnMeta>> columnsByTable,
            TokenProfile questionProfile
    ) {
        if (allTables.isEmpty()) {
            return List.of();
        }

        return allTables.stream()
                .map(table -> scoreTable(table, columnsByTable, questionProfile))
                .sorted(Comparator.comparingDouble((ScoredTable st) -> st.finalScore).reversed())
                .toList();
    }

    private ScoredTable scoreTable(
            TableMeta table,
            Map<String, List<ColumnMeta>> columnsByTable,
            TokenProfile questionProfile
    ) {
        String tableTokensText = table.tableName + " " + defaultString(table.tableDescription);
        double tableNlpScore = openNlpSimilarity(questionProfile, tableTokensText);
        List<ColumnMeta> tableColumns = columnsByTable.getOrDefault(tableKey(table.schemaName, table.tableName), List.of());
        double columnsNlpScore = tableColumns.stream()
                .mapToDouble(col -> {
                    String colText = col.columnName + " " + defaultString(col.columnComment) + " " + defaultString(col.semanticRole);
                    return openNlpSimilarity(questionProfile, colText);
                })
                .max()
                .orElse(0.0);
        String relationshipAwareTableText = tableTokensText + " " + tableColumns.stream()
                .map(col -> col.columnName + " " + defaultString(col.columnComment) + " " + defaultString(col.semanticRole))
                .collect(Collectors.joining(" "));
        double finalScore = openNlpSimilarity(questionProfile, relationshipAwareTableText);
        return new ScoredTable(table, finalScore, tableNlpScore, columnsNlpScore);
    }

    private double scoreColumn(
            ColumnMeta column,
            TokenProfile questionProfile
    ) {
        String columnTokensText = column.columnName + " " + defaultString(column.columnComment) + " " + defaultString(column.semanticRole);
        return openNlpSimilarity(questionProfile, columnTokensText);
    }

    private double weightedCosineSimilarity(Map<String, Double> left, Map<String, Double> right) {
        if (left == null || right == null || left.isEmpty() || right.isEmpty()) {
            return 0.0;
        }
        double dot = 0.0;
        for (Map.Entry<String, Double> entry : left.entrySet()) {
            double otherWeight = right.getOrDefault(entry.getKey(), 0.0);
            dot += entry.getValue() * otherWeight;
        }
        double normLeft = 0.0;
        for (double weight : left.values()) {
            normLeft += weight * weight;
        }
        double normRight = 0.0;
        for (double weight : right.values()) {
            normRight += weight * weight;
        }
        if (normLeft <= 0.0 || normRight <= 0.0) {
            return 0.0;
        }
        return dot / (Math.sqrt(normLeft) * Math.sqrt(normRight));
    }

    private double openNlpSimilarity(TokenProfile questionProfile, String metadataText) {
        if (posTagger == null || questionProfile == null || questionProfile.weights().isEmpty()) {
            return 0.0;
        }
        TokenProfile metadataProfile = buildTokenProfile(metadataText);
        return weightedCosineSimilarity(questionProfile.weights(), metadataProfile.weights());
    }

    private TokenProfile buildTokenProfile(String rawText) {
        if (rawText == null || rawText.isBlank() || posTagger == null) {
            return new TokenProfile(Set.of(), Map.of());
        }
        String normalized = normalizeText(rawText);
        List<String> tokens = tokenize(normalized);
        return analyzeTokenProfile(tokens);
    }

    private TokenProfile analyzeTokenProfile(List<String> tokens) {
        if (tokens == null || tokens.isEmpty() || posTagger == null) {
            return new TokenProfile(Set.of(), Map.of());
        }
        Sequence[] sequences = posTagger.topKSequences(tokens.toArray(String[]::new));
        if (sequences == null || sequences.length == 0) {
            return new TokenProfile(Set.of(), Map.of());
        }
        Sequence best = sequences[0];
        double[] probs = best.getProbs();
        LinkedHashSet<String> tokenSet = new LinkedHashSet<>();
        LinkedHashMap<String, Double> weights = new LinkedHashMap<>();
        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i);
            if (token == null || token.isBlank()) {
                continue;
            }
            double probability = (probs != null && i < probs.length) ? probs[i] : 0.0;
            double weight = probability;
            tokenSet.add(token);
            weights.merge(token, weight, Double::sum);
        }
        return new TokenProfile(tokenSet, weights);
    }

    private List<RelevantRelationship> selectRelationships(
            List<RelationshipMeta> relationships,
            List<ColumnMeta> selectedColumns,
            TokenProfile questionProfile
    ) {
        if (relationships.isEmpty()) {
            return List.of();
        }

        Set<String> selectedColumnKeys = selectedColumns.stream()
                .map(column -> columnKey(column.schemaName, column.tableName, column.columnName))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        List<RelevantRelationship> prioritized = new ArrayList<>();
        for (RelationshipMeta rel : relationships) {
            String relationText = rel.fromSchema + " " + rel.fromTable + " " + rel.fromColumn + " "
                    + rel.toSchema + " " + rel.toTable + " " + rel.toColumn + " "
                    + defaultString(rel.relationshipType);
            double confidence = openNlpSimilarity(questionProfile, relationText);
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

        return prioritized.stream()
                .sorted(Comparator.comparingDouble(RelevantRelationship::confidence).reversed())
                .toList();
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
        return value.toLowerCase(Locale.ROOT).trim();
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

    private record TokenProfile(Set<String> tokens, Map<String, Double> weights) {
    }
}








