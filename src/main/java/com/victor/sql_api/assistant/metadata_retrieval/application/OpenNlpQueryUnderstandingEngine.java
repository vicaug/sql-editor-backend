package com.victor.sql_api.assistant.metadata_retrieval.application;

import com.victor.sql_api.assistant.nl2sql.model.QueryIntent;
import com.victor.sql_api.assistant.nl2sql.model.QueryUnderstanding;
import org.apache.commons.text.similarity.JaroWinklerSimilarity;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.util.Span;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.text.Normalizer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Order(Ordered.HIGHEST_PRECEDENCE)
public class OpenNlpQueryUnderstandingEngine implements QueryUnderstandingEngine {
    private static final Pattern NON_ALNUM = Pattern.compile("[^a-z0-9 ]");
    private static final Pattern NLP_SAFE_CHARS = Pattern.compile("[^a-z0-9,;:.!?()\\- ]");
    private static final Pattern MULTI_SPACE = Pattern.compile("\\s+");
    private static final Pattern YEAR_PATTERN = Pattern.compile("\\b(20\\d{2})\\b");

    private static final Set<String> AGGREGATION_TERMS = Set.of(
            "total", "soma", "somar", "quantidade", "qtd", "numero", "media", "avg", "count", "contagem", "maximo", "minimo", "percentual", "taxa"
    );
    private static final Set<String> AGGREGATION_QUESTION_TERMS = Set.of("quanto", "quantos", "quantas", "numero");
    private static final Set<String> COMPARISON_TERMS = Set.of("comparar", "versus", "vs", "diferenca", "maior", "menor");
    private static final Set<String> TREND_TERMS = Set.of("evolucao", "mensal", "semanal", "diario", "anual", "historico", "ao longo");
    private static final Set<String> DETAIL_TERMS = Set.of("detalhe", "listar", "lista", "exibir", "mostrar");
    private static final Set<String> DETAIL_QUESTION_TERMS = Set.of("qual", "quais", "quem", "onde");
    private static final Set<String> GROUP_CONNECTORS = Set.of("por");
    private static final Set<String> DIMENSION_SEPARATORS = Set.of(",", "e", "vs", "versus");
    private static final Set<String> CLAUSE_BREAKERS = Set.of(
            "com", "sem", "onde", "quando", "que", "para", "entre", "durante", "desde", "ate", "até", "nos", "nas", "no", "na", "em"
    );
    private static final Set<String> DIMENSION_STOPWORDS = Set.of(
            "o", "a", "os", "as", "um", "uma", "de", "do", "da", "dos", "das", "por"
    );
    private static final double FUZZY_THRESHOLD = 0.90;

    private final JaroWinklerSimilarity similarity = new JaroWinklerSimilarity();
    private final SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
    private final POSTaggerME posTagger;

    public OpenNlpQueryUnderstandingEngine(OpenNlpPosTaggerProvider posTaggerProvider) {
        this.posTagger = posTaggerProvider.getPosTagger();
    }

    @Override
    public String name() {
        return "opennlp";
    }

    @Override
    public boolean isAvailable() {
        return posTagger != null;
    }

    @Override
    public QueryUnderstanding analyze(String question) {
        String lexicalNormalized = normalize(question);
        String nlpNormalized = normalizeForNlp(question);
        List<String> tokenList = tokenize(nlpNormalized);
        Set<String> tokens = new LinkedHashSet<>(tokenList);

        LinkedHashSet<String> metrics = pickTokens(tokens, AGGREGATION_TERMS);
        LinkedHashSet<String> dimensions = detectDimensionsWithPos(tokenList);
        LinkedHashSet<String> filters = new LinkedHashSet<>();
        LinkedHashSet<String> timeHints = new LinkedHashSet<>();
        Matcher yearMatcher = YEAR_PATTERN.matcher(lexicalNormalized);
        while (yearMatcher.find()) {
            String year = yearMatcher.group(1);
            filters.add("year=" + year);
            timeHints.add(year);
        }

        QueryIntent intent = detectIntent(lexicalNormalized, tokens, metrics, dimensions);
        boolean requiresAggregation = intent == QueryIntent.AGGREGATION || intent == QueryIntent.TREND || !metrics.isEmpty();
        boolean requiresJoin = !dimensions.isEmpty();

        double confidence = confidenceFromSignals(intent, tokenList, metrics, dimensions, filters, timeHints);

        return new QueryUnderstanding(
                intent,
                null,
                List.copyOf(metrics),
                List.copyOf(dimensions),
                List.copyOf(filters),
                List.copyOf(timeHints),
                requiresAggregation,
                requiresJoin,
                confidence
        );
    }

    private QueryIntent detectIntent(String normalized, Set<String> tokens, Set<String> metrics, Set<String> dimensions) {
        if (containsAnyPhrase(normalized, TREND_TERMS) || containsAnyToken(tokens, TREND_TERMS)) return QueryIntent.TREND;
        if (containsAnyToken(tokens, COMPARISON_TERMS)) return QueryIntent.COMPARISON;
        if (!dimensions.isEmpty() && containsAnyToken(tokens, AGGREGATION_QUESTION_TERMS)) return QueryIntent.AGGREGATION;
        if (!metrics.isEmpty()) return QueryIntent.AGGREGATION;
        if (containsAnyToken(tokens, DETAIL_QUESTION_TERMS)) return QueryIntent.DETAIL;
        if (containsAnyToken(tokens, DETAIL_TERMS)) return QueryIntent.DETAIL;
        return QueryIntent.UNKNOWN;
    }

    private LinkedHashSet<String> detectDimensionsWithPos(List<String> tokens) {
        LinkedHashSet<String> dimensions = new LinkedHashSet<>();
        List<String> currentDimensionTokens = new ArrayList<>();
        boolean collecting = false;
        String[] tags = posTagger.tag(tokens.toArray(String[]::new));

        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i);
            String tag = tags[i];

            if (GROUP_CONNECTORS.contains(token)) {
                flushDimension(currentDimensionTokens, dimensions);
                collecting = true;
                continue;
            }
            if (!collecting) continue;
            if (DIMENSION_SEPARATORS.contains(token)) {
                flushDimension(currentDimensionTokens, dimensions);
                continue;
            }
            if (isHardBoundaryToken(token) || isPunctuation(token)) {
                flushDimension(currentDimensionTokens, dimensions);
                collecting = false;
                continue;
            }
            if (isAllowedPosForDimension(tag)) {
                currentDimensionTokens.add(token);
            } else if (!currentDimensionTokens.isEmpty()) {
                flushDimension(currentDimensionTokens, dimensions);
                collecting = false;
            }
        }
        flushDimension(currentDimensionTokens, dimensions);
        if (!dimensions.isEmpty()) dimensions.add("agrupamento");
        return dimensions;
    }

    private boolean isAllowedPosForDimension(String tag) {
        if (tag == null || tag.isBlank()) return false;
        String normalizedTag = tag.toUpperCase(Locale.ROOT);
        return normalizedTag.contains("NOUN")
                || normalizedTag.contains("PROPN")
                || normalizedTag.contains("ADJ")
                || normalizedTag.contains("NUM");
    }

    private String cleanDimensionToken(String raw) {
        if (raw == null) return "";
        String cleaned = MULTI_SPACE.matcher(raw.trim()).replaceAll(" ");
        if (cleaned.isBlank()) return "";
        String[] parts = cleaned.split(" ");
        List<String> kept = new ArrayList<>();
        for (String part : parts) {
            if (!DIMENSION_STOPWORDS.contains(part)) kept.add(part);
        }
        return String.join(" ", kept).trim();
    }

    private void flushDimension(List<String> currentTokens, LinkedHashSet<String> dimensions) {
        if (currentTokens.isEmpty()) return;
        String cleaned = cleanDimensionToken(String.join(" ", currentTokens));
        if (!cleaned.isBlank()) dimensions.add(cleaned);
        currentTokens.clear();
    }

    private boolean isPunctuation(String token) {
        return token != null && token.length() == 1 && !Character.isLetterOrDigit(token.charAt(0));
    }

    private boolean isHardBoundaryToken(String token) {
        return token != null && CLAUSE_BREAKERS.contains(token);
    }

    private LinkedHashSet<String> pickTokens(Set<String> tokens, Set<String> dictionary) {
        LinkedHashSet<String> result = new LinkedHashSet<>();
        for (String token : tokens) {
            if (dictionary.contains(token) || isSimilarToAny(token, dictionary)) result.add(token);
        }
        return result;
    }

    private boolean containsAnyPhrase(String normalized, Set<String> candidates) {
        return candidates.stream().anyMatch(normalized::contains);
    }

    private boolean containsAnyToken(Set<String> tokens, Set<String> candidates) {
        return candidates.stream().anyMatch(candidate ->
                tokens.contains(candidate) || tokens.stream().anyMatch(token -> isSimilar(token, candidate))
        );
    }

    private boolean isSimilarToAny(String value, Set<String> candidates) {
        return candidates.stream().anyMatch(candidate -> isSimilar(value, candidate));
    }

    private boolean isSimilar(String left, String right) {
        return left != null && right != null && similarity.apply(left, right) >= FUZZY_THRESHOLD;
    }

    private double confidenceFromSignals(
            QueryIntent intent,
            List<String> tokenList,
            Collection<String> metrics,
            Collection<String> dimensions,
            Collection<String> filters,
            Collection<String> timeHints
    ) {
        if (tokenList == null || tokenList.isEmpty()) {
            return 0.20;
        }
        String[] tags = posTagger.tag(tokenList.toArray(String[]::new));
        int contentCount = 0;
        for (String tag : tags) {
            if (isAllowedPosForDimension(tag) || (tag != null && tag.toUpperCase(Locale.ROOT).contains("VERB"))) {
                contentCount++;
            }
        }
        double contentRatio = (double) contentCount / (double) tokenList.size();
        double confidence = intent == QueryIntent.UNKNOWN ? 0.30 : 0.55;
        confidence += Math.min(0.25, contentRatio * 0.25);
        confidence += Math.min(0.15, metrics.size() * 0.05);
        confidence += Math.min(0.12, dimensions.size() * 0.04);
        confidence += Math.min(0.08, (filters.size() + timeHints.size()) * 0.02);
        return Math.max(0.0, Math.min(confidence, 0.92));
    }

    private String normalize(String value) {
        if (value == null) return "";
        String noAccent = Normalizer.normalize(value.toLowerCase(Locale.ROOT), Normalizer.Form.NFD).replaceAll("\\p{M}", "");
        String clean = NON_ALNUM.matcher(noAccent).replaceAll(" ");
        return MULTI_SPACE.matcher(clean).replaceAll(" ").trim();
    }

    private String normalizeForNlp(String value) {
        if (value == null) return "";
        String noAccent = Normalizer.normalize(value.toLowerCase(Locale.ROOT), Normalizer.Form.NFD).replaceAll("\\p{M}", "");
        String clean = NLP_SAFE_CHARS.matcher(noAccent).replaceAll(" ");
        return MULTI_SPACE.matcher(clean).replaceAll(" ").trim();
    }

    private List<String> tokenize(String normalizedText) {
        if (normalizedText == null || normalizedText.isBlank()) return List.of();
        Span[] spans = tokenizer.tokenizePos(normalizedText);
        List<String> tokens = new ArrayList<>(spans.length);
        for (Span span : spans) {
            String token = normalizedText.substring(span.getStart(), span.getEnd()).trim();
            if (!token.isBlank()) tokens.add(token);
        }
        return tokens;
    }

}








