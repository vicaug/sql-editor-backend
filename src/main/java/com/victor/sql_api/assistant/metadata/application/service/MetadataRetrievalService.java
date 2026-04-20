package com.victor.sql_api.assistant.metadata.application.service;

import com.victor.sql_api.assistant.metadata.application.model.RetrievalConstraints;
import com.victor.sql_api.assistant.metadata.application.model.RetrievalRequest;
import com.victor.sql_api.assistant.metadata.domain.model.*;
import com.victor.sql_api.shared.exception.BadRequestException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.text.Normalizer;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class MetadataRetrievalService {

    private static final Pattern NON_ALNUM = Pattern.compile("[^a-z0-9_ ]");
    private static final Pattern MULTI_SPACE = Pattern.compile("\\s+");

    private static final Set<String> STOPWORDS = Set.of(
            "a", "o", "os", "as", "de", "da", "do", "das", "dos", "por", "com", "no", "na",
            "nos", "nas", "em", "qual", "quais", "me", "mostre", "mostrar", "quero", "que",
            "e", "ou", "um", "uma", "para", "como", "esta", "estao", "minha", "meu",
            "cada", "informacao", "informacoes", "detalhe", "detalhes"
    );
    private static final Set<String> GENERIC_TOKENS = Set.of(
            "dados", "dado", "tabela", "coluna", "valor", "nome", "codigo", "tipo", "total",
            "resultado", "lista", "listar", "consulta"
    );

    // Match strengths by type (explainable & tunable)
    private static final double MATCH_EXACT = 1.00;      // field == token
    private static final double MATCH_WORD = 0.80;       // whole word
    private static final double MATCH_PREFIX = 0.50;     // word startsWith(token)
    private static final double MATCH_SUBSTRING = 0.20;  // fallback

    // Field weights (balanced: name > semantic role/comment; column slightly higher than table)
    private static final double W_TABLE_NAME = 3.4;
    private static final double W_TABLE_TEXT = 1.7;      // comment + description_llm (support context)
    private static final double W_COLUMN_NAME = 3.8;
    private static final double W_COLUMN_TEXT = 1.9;     // comment
    private static final double W_COLUMN_ROLE = 2.2;     // semantic_role (strong semantic clue)

    // Light fallback to recover tables that only appear through column evidence
    private static final int TABLE_FALLBACK_TOP_COLUMNS = 3;
    private static final double TABLE_FALLBACK_FACTOR = 0.35;
    private static final double TABLE_FALLBACK_SCORE_CAP = 2.8;
    private static final int TABLE_FALLBACK_MAX_COLUMN_ROWS = 1200;
    private static final double TABLE_SELECTION_MIN_SCORE = 1.0;
    private static final double TABLE_SELECTION_MIN_RELATIVE = 0.55;

    // Relationship path constraints
    private static final int MAX_RELATIONSHIP_HOPS = 3;
    private static final double HOP_PENALTY = 0.10; // per additional hop

    private final JdbcTemplate jdbcTemplate;

    public MetadataRetrievalService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public MetadataContext retrieve(RetrievalRequest request) {
        if (request == null || request.question() == null || request.question().trim().isEmpty()) {
            throw new BadRequestException("METADATA_QUESTION_EMPTY", "A pergunta para retrieval de metadata não pode ser vazia.");
        }

        RetrievalConstraints constraints = request.effectiveConstraints();
        Analysis analysis = analyzeQuestion(request.question());

        List<TableMeta> tables = fetchTables();
        List<TableCandidate> tableCandidates = scoreTables(analysis, tables);
        List<TableCandidate> selectedTables = selectTables(tableCandidates, constraints.maxTables());
        selectedTables = enrichTablesWithColumnFallback(selectedTables, analysis, constraints.maxTables());
        List<ColumnMeta> columns = fetchColumns(selectedTables);
        List<ColumnCandidate> columnCandidates = scoreColumns(analysis, columns);
        List<ColumnCandidate> selectedColumns = selectColumns(columnCandidates, selectedTables, constraints);
        List<RelationshipMeta> relationships = fetchRelationships(selectedTables);
        List<RelevantRelationship> selectedRelationships = resolveRelationships(
                selectedTables,
                selectedColumns,
                relationships,
                constraints.maxRelationships()
        );

        List<RelevantTable> relevantTables = selectedTables.stream()
                .map(table -> new RelevantTable(
                        table.schemaName,
                        table.tableName,
                        truncate(table.tableTextOriginal, 240),
                        table.score
                ))
                .toList();

        List<RelevantColumn> relevantColumns = selectedColumns.stream()
                .map(column -> new RelevantColumn(
                        column.schemaName,
                        column.tableName,
                        column.columnName,
                        column.dataType,
                        column.semanticRoleOriginal,
                        truncate(column.columnCommentOriginal, 220),
                        column.primaryKey,
                        column.foreignKey,
                        column.score
                ))
                .toList();

        List<RetrievalHint> hints = buildGenericHints(relevantColumns, selectedRelationships);

        RetrievalDiagnostics diagnostics = new RetrievalDiagnostics(
                analysis.tokens,
                tables.size(),
                columns.size(),
                relationships.size(),
                relevantTables.size(),
                relevantColumns.size(),
                selectedRelationships.size(),
                selectedTables.stream().map(TableCandidate::debugLine).toList(),
                selectedColumns.stream().map(ColumnCandidate::debugLine).toList(),
                selectedRelationships.stream()
                        .map(rel -> rel.fromSchema() + "." + rel.fromTable() + "." + rel.fromColumn()
                                + " -> "
                                + rel.toSchema() + "." + rel.toTable() + "." + rel.toColumn()
                                + " | conf=" + format(rel.confidence()))
                        .toList()
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

    private Analysis analyzeQuestion(String question) {
        String normalizedQuestion = normalizeText(question);
        Set<String> tokens = tokenize(normalizedQuestion).stream()
                .filter(token -> !STOPWORDS.contains(token))
                .filter(token -> token.length() > 1)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return new Analysis(tokens);
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
            TableMeta mapped = mapTableMeta(row);
            if (mapped != null) {
                result.add(mapped);
            }
        }
        return result;
    }

    private List<ColumnMeta> fetchColumns(List<TableCandidate> selectedTables) {
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
            ColumnMeta mapped = mapColumnMeta(row);
            if (mapped != null) {
                result.add(mapped);
            }
        }
        return result;
    }

    private List<RelationshipMeta> fetchRelationships(List<TableCandidate> selectedTables) {
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
                    relationship_type,
                    join_type_recommended,
                    relationship_comment
                from eqt_metadata.md_relationship
                where (%s)
                  and (%s)
                """.formatted(sourceFilter, targetFilter);

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, params.toArray());
        List<RelationshipMeta> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            RelationshipMeta mapped = mapRelationshipMeta(row);
            if (mapped != null) {
                result.add(mapped);
            }
        }
        return result;
    }

    private List<TableCandidate> scoreTables(Analysis analysis, List<TableMeta> tables) {
        List<TableCandidate> candidates = new ArrayList<>();
        for (TableMeta table : tables) {
            FieldScore nameScore = scoreField(analysis.tokens, table.tableNameNormalized(), table.tableNameWords(), W_TABLE_NAME, "table_name");
            FieldScore textScore = scoreField(analysis.tokens, table.tableTextNormalized(), table.tableTextWords(), W_TABLE_TEXT, "table_text");

            double total = nameScore.score + textScore.score;
            if (total <= 0) {
                continue;
            }

            Set<String> matchedTerms = new LinkedHashSet<>();
            matchedTerms.addAll(nameScore.matchedTerms);
            matchedTerms.addAll(textScore.matchedTerms);

            candidates.add(new TableCandidate(
                    table.schemaName,
                    table.tableNameOriginal,
                    table.tableTextOriginal,
                    total,
                    matchedTerms,
                    Map.of(
                            "table_name", nameScore.score,
                            "table_text", textScore.score
                    ),
                    "lexical match in metadata table fields"
            ));
        }
        return candidates.stream()
                .sorted(Comparator.comparingDouble((TableCandidate c) -> c.score).reversed())
                .toList();
    }

    private List<ColumnCandidate> scoreColumns(Analysis analysis, List<ColumnMeta> columns) {
        List<ColumnCandidate> candidates = new ArrayList<>();
        for (ColumnMeta column : columns) {
            FieldScore nameScore = scoreField(analysis.tokens, column.columnNameNormalized(), column.columnNameWords(), W_COLUMN_NAME, "column_name");
            FieldScore textScore = scoreField(analysis.tokens, column.columnCommentNormalized(), column.columnCommentWords(), W_COLUMN_TEXT, "column_comment");
            FieldScore roleScore = scoreField(analysis.tokens, column.semanticRoleNormalized(), column.semanticRoleWords(), W_COLUMN_ROLE, "semantic_role");

            boolean hasLexicalEvidence = !nameScore.matchedTerms.isEmpty()
                    || !textScore.matchedTerms.isEmpty()
                    || !roleScore.matchedTerms.isEmpty();
            double technicalBoost = (column.primaryKey || column.foreignKey) && hasLexicalEvidence ? 0.25 : 0.0;
            double total = nameScore.score + textScore.score + roleScore.score + technicalBoost;
            if (total <= 0) {
                continue;
            }

            Set<String> matchedTerms = new LinkedHashSet<>();
            matchedTerms.addAll(nameScore.matchedTerms);
            matchedTerms.addAll(textScore.matchedTerms);
            matchedTerms.addAll(roleScore.matchedTerms);

            LinkedHashMap<String, Double> breakdown = new LinkedHashMap<>();
            breakdown.put("column_name", nameScore.score);
            breakdown.put("column_comment", textScore.score);
            breakdown.put("semantic_role", roleScore.score);
            if (technicalBoost > 0) {
                breakdown.put("key_boost", technicalBoost);
            }

            candidates.add(new ColumnCandidate(
                    column.schemaName,
                    column.tableNameOriginal,
                    column.columnNameOriginal,
                    column.dataType,
                    column.columnCommentOriginal,
                    column.semanticRoleOriginal,
                    column.primaryKey,
                    column.foreignKey,
                    total,
                    matchedTerms,
                    breakdown,
                    "lexical match in metadata column fields"
            ));
        }
        return candidates.stream()
                .sorted(Comparator.comparingDouble((ColumnCandidate c) -> c.score).reversed())
                .toList();
    }

    private FieldScore scoreField(
            Set<String> tokens,
            String normalizedField,
            Set<String> fieldWords,
            double fieldWeight,
            String fieldName
    ) {
        if (normalizedField.isBlank() || tokens.isEmpty()) {
            return FieldScore.empty(fieldName);
        }
        double score = 0;
        Set<String> matched = new LinkedHashSet<>();
        for (String token : tokens) {
            MatchType matchType = bestMatchType(token, normalizedField, fieldWords);
            if (matchType == null) {
                continue;
            }
            double specificity = tokenSpecificityMultiplier(token, matchType);
            if (specificity <= 0) {
                continue;
            }
            score += fieldWeight * matchType.strength * specificity;
            matched.add(token);
        }
        return new FieldScore(fieldName, score, matched);
    }

    private double tokenSpecificityMultiplier(String token, MatchType matchType) {
        int length = token.length();

        // Very short tokens are too noisy for weak match types.
        if (length <= 2 && (matchType == MatchType.PREFIX || matchType == MatchType.SUBSTRING)) {
            return 0.0;
        }

        // Short tokens still useful for exact/word, but less trustworthy.
        if (length <= 3) {
            if (matchType == MatchType.EXACT || matchType == MatchType.WORD) {
                return 0.70;
            }
            return 0.35;
        }

        // Generic tokens should contribute less on weak lexical matches.
        if (GENERIC_TOKENS.contains(token)) {
            if (matchType == MatchType.SUBSTRING) {
                return 0.25;
            }
            if (matchType == MatchType.PREFIX) {
                return 0.45;
            }
            return 0.70;
        }

        // Mild penalty for weak matches even on normal tokens.
        if (matchType == MatchType.SUBSTRING) {
            return 0.75;
        }
        if (matchType == MatchType.PREFIX) {
            return 0.90;
        }
        return 1.0;
    }

    private MatchType bestMatchType(String token, String normalizedField, Set<String> fieldWords) {
        if (token.isBlank() || normalizedField.isBlank()) {
            return null;
        }
        if (normalizedField.equals(token)) {
            return MatchType.EXACT;
        }
        if (fieldWords.contains(token)) {
            return MatchType.WORD;
        }
        if (token.length() >= 3 && fieldWords.stream().anyMatch(word -> word.startsWith(token))) {
            return MatchType.PREFIX;
        }
        if (token.length() >= 4 && normalizedField.contains(token)) {
            return MatchType.SUBSTRING;
        }
        return null;
    }

    private List<TableCandidate> selectTables(
            List<TableCandidate> tableCandidates,
            int maxTables
    ) {
        if (tableCandidates.isEmpty()) {
            return List.of();
        }

        double topScore = tableCandidates.stream()
                .mapToDouble(TableCandidate::score)
                .max()
                .orElse(0.0);
        double relativeCutoff = topScore * TABLE_SELECTION_MIN_RELATIVE;
        double effectiveCutoff = Math.max(TABLE_SELECTION_MIN_SCORE, relativeCutoff);

        List<TableCandidate> filtered = tableCandidates.stream()
                .filter(table -> table.score >= effectiveCutoff)
                .sorted(Comparator.comparingDouble((TableCandidate t) -> t.score).reversed())
                .toList();

        if (filtered.isEmpty()) {
            filtered = tableCandidates.stream()
                    .sorted(Comparator.comparingDouble((TableCandidate t) -> t.score).reversed())
                    .toList();
        }

        return filtered.stream()
                .limit(Math.max(maxTables, 1))
                .toList();
    }

    private List<TableCandidate> enrichTablesWithColumnFallback(
            List<TableCandidate> selectedTables,
            Analysis analysis,
            int maxTables
    ) {
        int targetSize = Math.max(maxTables, 1);
        if (selectedTables.size() >= targetSize || analysis.tokens.isEmpty()) {
            return selectedTables;
        }

        Set<String> selectedTableSet = selectedTables.stream()
                .map(TableCandidate::fullName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<String> coveredTerms = selectedTables.stream()
                .flatMap(table -> table.matchedTerms.stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        List<ColumnMeta> fallbackColumns = fetchFallbackColumns(analysis.tokens);
        if (fallbackColumns.isEmpty()) {
            return selectedTables;
        }

        List<ColumnCandidate> fallbackCandidates = scoreColumns(analysis, fallbackColumns).stream()
                .filter(candidate -> !selectedTableSet.contains(candidate.tableFullName()))
                .toList();
        if (fallbackCandidates.isEmpty()) {
            return selectedTables;
        }

        Map<String, List<ColumnCandidate>> byTable = fallbackCandidates.stream()
                .collect(Collectors.groupingBy(ColumnCandidate::tableFullName));

        List<TableCandidate> inferred = new ArrayList<>();
        for (Map.Entry<String, List<ColumnCandidate>> entry : byTable.entrySet()) {
            List<ColumnCandidate> cols = entry.getValue().stream()
                    .sorted(Comparator.comparingDouble((ColumnCandidate c) -> c.score).reversed())
                    .limit(TABLE_FALLBACK_TOP_COLUMNS)
                    .toList();
            if (cols.isEmpty()) {
                continue;
            }

            double avgTop = cols.stream().mapToDouble(ColumnCandidate::score).average().orElse(0);
            double score = Math.min(avgTop * TABLE_FALLBACK_FACTOR, TABLE_FALLBACK_SCORE_CAP);
            if (score <= 0) {
                continue;
            }

            ColumnCandidate seed = cols.get(0);
            Set<String> matchedTerms = cols.stream()
                    .flatMap(column -> column.matchedTerms.stream())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            boolean addsNewTerms = matchedTerms.stream().anyMatch(term -> !coveredTerms.contains(term));
            if (!addsNewTerms) {
                continue;
            }
            LinkedHashMap<String, Double> breakdown = new LinkedHashMap<>();
            breakdown.put("base_score", 0.0);
            breakdown.put("column_fallback_avg_top" + TABLE_FALLBACK_TOP_COLUMNS, avgTop);
            breakdown.put("column_fallback_score", score);

            inferred.add(new TableCandidate(
                    seed.schemaName,
                    seed.tableName,
                    "",
                    score,
                    matchedTerms,
                    breakdown,
                    "fallback inferred from column lexical matches"
            ));
        }

        if (inferred.isEmpty()) {
            return selectedTables;
        }

        List<TableCandidate> merged = new ArrayList<>(selectedTables);
        inferred.stream()
                .sorted(Comparator.comparingDouble((TableCandidate t) -> t.score).reversed())
                .limit(Math.max(0, targetSize - selectedTables.size()))
                .forEach(merged::add);
        return merged;
    }

    private List<ColumnMeta> fetchFallbackColumns(Set<String> tokens) {
        List<String> effectiveTokens = tokens.stream()
                .filter(token -> token.length() >= 3)
                .limit(8)
                .toList();
        if (effectiveTokens.isEmpty()) {
            return List.of();
        }

        List<Object> params = new ArrayList<>();
        StringBuilder tokenFilter = new StringBuilder();
        for (int i = 0; i < effectiveTokens.size(); i++) {
            if (i > 0) {
                tokenFilter.append(" or ");
            }
            tokenFilter.append("""
                    c.column_name ilike ? 
                    or coalesce(c.column_comment, '') ilike ? 
                    or coalesce(c.semantic_role, '') ilike ?
                    """);
            String pattern = "%" + effectiveTokens.get(i) + "%";
            params.add(pattern);
            params.add(pattern);
            params.add(pattern);
        }
        params.add(TABLE_FALLBACK_MAX_COLUMN_ROWS);

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
                limit ?
                """.formatted(tokenFilter);

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, params.toArray());
        List<ColumnMeta> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            ColumnMeta mapped = mapColumnMeta(row);
            if (mapped != null) {
                result.add(mapped);
            }
        }
        return result;
    }

    private String buildTableFilter(
            String schemaColumn,
            String tableColumn,
            List<TableCandidate> selectedTables,
            List<Object> params
    ) {
        StringBuilder filter = new StringBuilder();
        for (int i = 0; i < selectedTables.size(); i++) {
            if (i > 0) {
                filter.append(" or ");
            }
            filter.append("(")
                    .append(schemaColumn)
                    .append(" = ? and ")
                    .append(tableColumn)
                    .append(" = ?)");
            TableCandidate table = selectedTables.get(i);
            params.add(table.schemaName);
            params.add(table.tableName);
        }
        return filter.toString();
    }

    private List<ColumnCandidate> selectColumns(
            List<ColumnCandidate> columnCandidates,
            List<TableCandidate> selectedTables,
            RetrievalConstraints constraints
    ) {
        Set<String> selectedTableSet = selectedTables.stream().map(TableCandidate::fullName).collect(Collectors.toSet());

        Map<String, List<ColumnCandidate>> byTable = columnCandidates.stream()
                .filter(col -> selectedTableSet.contains(col.tableFullName()))
                .collect(Collectors.groupingBy(ColumnCandidate::tableFullName));

        for (Map.Entry<String, List<ColumnCandidate>> entry : byTable.entrySet()) {
            entry.setValue(entry.getValue().stream()
                    .sorted(Comparator
                            .comparingInt((ColumnCandidate c) -> semanticPriority(c.semanticRoleOriginal, c.primaryKey, c.foreignKey)).reversed()
                            .thenComparing(Comparator.comparingDouble((ColumnCandidate c) -> c.score).reversed())
                    )
                    .toList());
        }

        List<ColumnCandidate> selected = new ArrayList<>();
        Map<String, Integer> cursorByTable = new HashMap<>();
        Map<String, Integer> countByTable = new HashMap<>();

        // Pass 1: ensure each selected table contributes at least one top column
        for (TableCandidate table : selectedTables) {
            String key = table.fullName();
            List<ColumnCandidate> cols = byTable.getOrDefault(key, List.of());
            if (!cols.isEmpty() && selected.size() < constraints.maxTotalColumns()) {
                selected.add(cols.get(0));
                cursorByTable.put(key, 1);
                countByTable.put(key, 1);
            }
        }

        // Pass 2: round-robin fill respecting per-table and global limits
        boolean added;
        do {
            added = false;
            for (TableCandidate table : selectedTables) {
                if (selected.size() >= constraints.maxTotalColumns()) {
                    break;
                }
                String key = table.fullName();
                int used = countByTable.getOrDefault(key, 0);
                if (used >= constraints.maxColumnsPerTable()) {
                    continue;
                }
                List<ColumnCandidate> cols = byTable.getOrDefault(key, List.of());
                int cursor = cursorByTable.getOrDefault(key, 0);
                if (cursor >= cols.size()) {
                    continue;
                }
                selected.add(cols.get(cursor));
                cursorByTable.put(key, cursor + 1);
                countByTable.put(key, used + 1);
                added = true;
            }
        } while (added && selected.size() < constraints.maxTotalColumns());

        return selected;
    }

    private List<RelevantRelationship> resolveRelationships(
            List<TableCandidate> selectedTables,
            List<ColumnCandidate> selectedColumns,
            List<RelationshipMeta> relationships,
            int maxRelationships
    ) {
        if (selectedTables.size() <= 1 || maxRelationships <= 0) {
            return List.of();
        }

        Set<String> selectedTableSet = selectedTables.stream()
                .map(TableCandidate::fullName)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<String> selectedColumnSet = selectedColumns.stream()
                .map(ColumnCandidate::columnFullName)
                .collect(Collectors.toSet());

        Map<String, List<RelationshipMeta>> graph = buildGraph(relationships);
        LinkedHashMap<String, RelevantRelationship> resolved = new LinkedHashMap<>();

        // Prefer direct relationships first
        List<RelationshipMeta> direct = relationships.stream()
                .filter(rel -> selectedTableSet.contains(rel.fromTableFullName()) && selectedTableSet.contains(rel.toTableFullName()))
                .sorted(Comparator.comparingDouble((RelationshipMeta rel) -> relationshipConfidence(rel, 1, selectedColumnSet)).reversed())
                .toList();

        for (RelationshipMeta rel : direct) {
            if (resolved.size() >= maxRelationships) {
                break;
            }
            double confidence = relationshipConfidence(rel, 1, selectedColumnSet);
            resolved.putIfAbsent(rel.key(), rel.toRelevant(confidence));
        }

        // If needed, connect disconnected pairs with best path under hop limits
        List<String> tables = new ArrayList<>(selectedTableSet);
        for (int i = 0; i < tables.size() - 1 && resolved.size() < maxRelationships; i++) {
            String from = tables.get(i);
            String to = tables.get(i + 1);
            List<RelationshipMeta> path = bestPath(from, to, graph, selectedColumnSet);
            for (int hop = 0; hop < path.size(); hop++) {
                if (resolved.size() >= maxRelationships) {
                    break;
                }
                RelationshipMeta rel = path.get(hop);
                double confidence = relationshipConfidence(rel, hop + 1, selectedColumnSet);
                if (confidence < 0.35) {
                    continue; // discard fragile links
                }
                resolved.putIfAbsent(rel.key(), rel.toRelevant(confidence));
            }
        }

        return new ArrayList<>(resolved.values());
    }

    private List<RelationshipMeta> bestPath(
            String from,
            String to,
            Map<String, List<RelationshipMeta>> graph,
            Set<String> selectedColumnSet
    ) {
        if (from.equals(to)) {
            return List.of();
        }

        record State(String table, double score, int hops) {}
        PriorityQueue<State> pq = new PriorityQueue<>(Comparator.comparingDouble((State s) -> s.score).reversed());
        Map<String, Double> bestScoreByNode = new HashMap<>();
        Map<String, RelationshipMeta> parentByNode = new HashMap<>();

        pq.add(new State(from, 0, 0));
        bestScoreByNode.put(from, 0.0);

        while (!pq.isEmpty()) {
            State current = pq.poll();
            if (current.table.equals(to)) {
                break;
            }
            if (current.hops >= MAX_RELATIONSHIP_HOPS) {
                continue;
            }
            for (RelationshipMeta edge : graph.getOrDefault(current.table, List.of())) {
                int nextHops = current.hops + 1;
                if (nextHops > MAX_RELATIONSHIP_HOPS) {
                    continue;
                }
                double edgeScore = relationshipConfidence(edge, nextHops, selectedColumnSet);
                double candidateScore = current.score + edgeScore - (nextHops > 1 ? HOP_PENALTY : 0);

                String nextTable = edge.toTableFullName();
                double best = bestScoreByNode.getOrDefault(nextTable, -9999.0);
                if (candidateScore > best) {
                    bestScoreByNode.put(nextTable, candidateScore);
                    parentByNode.put(nextTable, edge);
                    pq.add(new State(nextTable, candidateScore, nextHops));
                }
            }
        }

        if (!parentByNode.containsKey(to)) {
            return List.of();
        }

        List<RelationshipMeta> path = new ArrayList<>();
        String cursor = to;
        while (!cursor.equals(from)) {
            RelationshipMeta edge = parentByNode.get(cursor);
            if (edge == null) {
                return List.of();
            }
            path.add(edge);
            cursor = edge.fromTableFullName();
        }
        Collections.reverse(path);
        return path;
    }

    private double relationshipConfidence(RelationshipMeta rel, int hops, Set<String> selectedColumnSet) {
        double confidence = 0.70;
        String relationshipType = defaultString(rel.relationshipType).toUpperCase(Locale.ROOT);
        String joinType = defaultString(rel.joinTypeRecommended).toUpperCase(Locale.ROOT);

        if (relationshipType.contains("FOREIGN_KEY")) {
            confidence += 0.16;
        }
        if (joinType.equals("INNER") || joinType.equals("LEFT")) {
            confidence += 0.04;
        }
        if (selectedColumnSet.contains(rel.fromColumnFullName()) || selectedColumnSet.contains(rel.toColumnFullName())) {
            confidence += 0.05;
        }
        if (!defaultString(rel.relationshipComment).isBlank()) {
            confidence += 0.02;
        }

        confidence -= Math.max(0, hops - 1) * HOP_PENALTY;
        return Math.max(0.0, Math.min(confidence, 0.99));
    }

    private Map<String, List<RelationshipMeta>> buildGraph(List<RelationshipMeta> relationships) {
        Map<String, List<RelationshipMeta>> graph = new HashMap<>();
        for (RelationshipMeta rel : relationships) {
            graph.computeIfAbsent(rel.fromTableFullName(), key -> new ArrayList<>()).add(rel);
            graph.computeIfAbsent(rel.toTableFullName(), key -> new ArrayList<>()).add(rel.reverse());
        }
        return graph;
    }

    private List<RetrievalHint> buildGenericHints(
            List<RelevantColumn> columns,
            List<RelevantRelationship> relationships
    ) {
        List<RetrievalHint> hints = new ArrayList<>();
        Set<String> roles = columns.stream()
                .map(RelevantColumn::semanticRole)
                .filter(Objects::nonNull)
                .map(role -> role.trim().toUpperCase(Locale.ROOT))
                .filter(role -> !role.isBlank())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        roles.forEach(role -> hints.add(new RetrievalHint("SEMANTIC_ROLE", role, 0.85)));
        if (!relationships.isEmpty()) {
            hints.add(new RetrievalHint("RELATIONSHIP_HINT", "JOIN_PATH_AVAILABLE", 0.90));
        }
        return hints;
    }

    private int semanticPriority(String semanticRole, boolean isPk, boolean isFk) {
        if (isPk || isFk) {
            return 4;
        }
        String role = defaultString(semanticRole).toUpperCase(Locale.ROOT);
        if (role.contains("PRIMARY_KEY") || role.contains("FOREIGN_KEY")) {
            return 4;
        }
        if (role.contains("METRIC")) {
            return 3;
        }
        if (role.contains("DATE") || role.contains("TIME")) {
            return 2;
        }
        if (role.contains("ATTRIBUTE")) {
            return 1;
        }
        return 0;
    }

    private static String normalizeText(String value) {
        if (value == null) {
            return "";
        }
        String noAccent = Normalizer.normalize(value.toLowerCase(Locale.ROOT), Normalizer.Form.NFD)
                .replaceAll("\\p{M}", "");
        String clean = NON_ALNUM.matcher(noAccent).replaceAll(" ");
        return MULTI_SPACE.matcher(clean).replaceAll(" ").trim();
    }

    private static Set<String> tokenize(String normalizedText) {
        if (normalizedText == null || normalizedText.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(normalizedText.split(" "))
                .filter(token -> !token.isBlank())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private String truncate(String value, int maxLength) {
        if (value == null) {
            return "";
        }
        if (value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength - 3) + "...";
    }

    private String joinText(String first, String second) {
        String a = defaultString(first).trim();
        String b = defaultString(second).trim();
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

    private TableMeta mapTableMeta(Map<String, Object> row) {
        String schema = getString(row, "table_schema", "schema_name", "schema");
        String table = getString(row, "table_name", "name");
        if (schema == null || table == null) {
            return null;
        }
        String textOriginal = joinText(
                getString(row, "table_comment", "comment"),
                getString(row, "table_description_llm", "description_llm")
        );
        return new TableMeta(schema, table, textOriginal);
    }

    private ColumnMeta mapColumnMeta(Map<String, Object> row) {
        String schema = getString(row, "table_schema", "schema_name", "schema");
        String table = getString(row, "table_name");
        String column = getString(row, "column_name", "name");
        if (schema == null || table == null || column == null) {
            return null;
        }
        return new ColumnMeta(
                schema,
                table,
                column,
                getString(row, "data_type", "column_type", "type"),
                getString(row, "column_comment", "comment"),
                getString(row, "semantic_role", "role"),
                getBoolean(row, "is_primary_key", "is_pk", "primary_key", "pk"),
                getBoolean(row, "is_foreign_key", "is_fk", "foreign_key", "fk")
        );
    }

    private RelationshipMeta mapRelationshipMeta(Map<String, Object> row) {
        String fromSchema = getString(row, "source_table_schema", "from_schema");
        String fromTable = getString(row, "source_table_name", "from_table");
        String fromColumn = getString(row, "source_column_name", "from_column");
        String toSchema = getString(row, "target_table_schema", "to_schema");
        String toTable = getString(row, "target_table_name", "to_table");
        String toColumn = getString(row, "target_column_name", "to_column");
        if (fromSchema == null || fromTable == null || fromColumn == null || toSchema == null || toTable == null || toColumn == null) {
            return null;
        }
        return new RelationshipMeta(
                fromSchema,
                fromTable,
                fromColumn,
                toSchema,
                toTable,
                toColumn,
                getString(row, "relationship_type"),
                getString(row, "join_type_recommended"),
                getString(row, "relationship_comment")
        );
    }

    private String format(double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private enum MatchType {
        EXACT(MATCH_EXACT),
        WORD(MATCH_WORD),
        PREFIX(MATCH_PREFIX),
        SUBSTRING(MATCH_SUBSTRING);

        private final double strength;

        MatchType(double strength) {
            this.strength = strength;
        }
    }

    private record Analysis(Set<String> tokens) {
    }

    private record FieldScore(String field, double score, Set<String> matchedTerms) {
        static FieldScore empty(String field) {
            return new FieldScore(field, 0, Set.of());
        }
    }

    private record TableMeta(String schemaName, String tableNameOriginal, String tableTextOriginal) {
        String tableNameNormalized() {
            return normalizeText(tableNameOriginal);
        }

        Set<String> tableNameWords() {
            return tokenize(tableNameNormalized());
        }

        String tableTextNormalized() {
            return normalizeText(tableTextOriginal);
        }

        Set<String> tableTextWords() {
            return tokenize(tableTextNormalized());
        }
    }

    private record ColumnMeta(
            String schemaName,
            String tableNameOriginal,
            String columnNameOriginal,
            String dataType,
            String columnCommentOriginal,
            String semanticRoleOriginal,
            boolean primaryKey,
            boolean foreignKey
    ) {
        String columnNameNormalized() {
            return normalizeText(columnNameOriginal);
        }

        Set<String> columnNameWords() {
            return tokenize(columnNameNormalized());
        }

        String columnCommentNormalized() {
            return normalizeText(columnCommentOriginal);
        }

        Set<String> columnCommentWords() {
            return tokenize(columnCommentNormalized());
        }

        String semanticRoleNormalized() {
            return normalizeText(semanticRoleOriginal);
        }

        Set<String> semanticRoleWords() {
            return tokenize(semanticRoleNormalized());
        }
    }

    private record RelationshipMeta(
            String fromSchema,
            String fromTable,
            String fromColumn,
            String toSchema,
            String toTable,
            String toColumn,
            String relationshipType,
            String joinTypeRecommended,
            String relationshipComment
    ) {
        String fromTableFullName() {
            return fromSchema + "." + fromTable;
        }

        String toTableFullName() {
            return toSchema + "." + toTable;
        }

        String fromColumnFullName() {
            return fromSchema + "." + fromTable + "." + fromColumn;
        }

        String toColumnFullName() {
            return toSchema + "." + toTable + "." + toColumn;
        }

        String key() {
            return fromColumnFullName() + "->" + toColumnFullName();
        }

        RelationshipMeta reverse() {
            return new RelationshipMeta(
                    toSchema,
                    toTable,
                    toColumn,
                    fromSchema,
                    fromTable,
                    fromColumn,
                    relationshipType,
                    joinTypeRecommended,
                    relationshipComment
            );
        }

        RelevantRelationship toRelevant(double confidence) {
            return new RelevantRelationship(
                    fromSchema,
                    fromTable,
                    fromColumn,
                    toSchema,
                    toTable,
                    toColumn,
                    relationshipType,
                    confidence
            );
        }
    }

    private record TableCandidate(
            String schemaName,
            String tableName,
            String tableTextOriginal,
            double score,
            Set<String> matchedTerms,
            Map<String, Double> scoreBreakdown,
            String inclusionReason
    ) {
        String fullName() {
            return schemaName + "." + tableName;
        }

        String debugLine() {
            return fullName()
                    + " | score=" + String.format(Locale.ROOT, "%.3f", score)
                    + " | matched=" + matchedTerms
                    + " | breakdown=" + scoreBreakdown
                    + " | reason=" + inclusionReason;
        }
    }

    private record ColumnCandidate(
            String schemaName,
            String tableName,
            String columnName,
            String dataType,
            String columnCommentOriginal,
            String semanticRoleOriginal,
            boolean primaryKey,
            boolean foreignKey,
            double score,
            Set<String> matchedTerms,
            Map<String, Double> scoreBreakdown,
            String inclusionReason
    ) {
        String tableFullName() {
            return schemaName + "." + tableName;
        }

        String columnFullName() {
            return schemaName + "." + tableName + "." + columnName;
        }

        String debugLine() {
            return columnFullName()
                    + " | score=" + String.format(Locale.ROOT, "%.3f", score)
                    + " | matched=" + matchedTerms
                    + " | breakdown=" + scoreBreakdown
                    + " | reason=" + inclusionReason;
        }
    }
}
