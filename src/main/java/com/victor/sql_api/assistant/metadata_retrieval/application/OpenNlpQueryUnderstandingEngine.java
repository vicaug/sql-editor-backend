package com.victor.sql_api.assistant.metadata_retrieval.application;

import com.victor.sql_api.assistant.nl2sql.model.QueryIntent;
import com.victor.sql_api.assistant.nl2sql.model.QueryUnderstanding;
import opennlp.tools.doccat.DocumentCategorizerME;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.util.Sequence;
import opennlp.tools.util.Span;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.text.Normalizer;
import java.util.*;
import java.util.regex.Pattern;

@Service
@Order(Ordered.HIGHEST_PRECEDENCE)
public class OpenNlpQueryUnderstandingEngine implements QueryUnderstandingEngine {
    private static final Pattern NLP_SAFE_CHARS = Pattern.compile("[^a-z0-9,;:.!?()\\- ]");
    private static final Pattern MULTI_SPACE = Pattern.compile("\\s+");
    private final SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
    private final POSTaggerME posTagger;
    private final DocumentCategorizerME intentCategorizer;

    public OpenNlpQueryUnderstandingEngine(
            OpenNlpPosTaggerProvider posTaggerProvider,
            OpenNlpIntentCategorizerProvider intentCategorizerProvider
    ) {
        this.posTagger = posTaggerProvider.getPosTagger();
        this.intentCategorizer = intentCategorizerProvider.getCategorizer();
    }

    @Override
    public String name() {
        return "opennlp";
    }

    @Override
    public boolean isAvailable() {
        return posTagger != null && intentCategorizer != null;
    }

    @Override
    public QueryUnderstanding analyze(String question) {
        if (posTagger == null || intentCategorizer == null) {
            return unavailableUnderstanding(question);
        }
        String nlpNormalized = normalizeForNlp(question);
        List<String> tokenList = tokenize(nlpNormalized);
        if (tokenList.isEmpty()) {
            return unavailableUnderstanding(question);
        }

        PosAnalysis posAnalysis = analyzePos(tokenList);
        String[] tags = posAnalysis.tags();

        LinkedHashSet<String> semanticTerms = extractSemanticTerms(tokenList, tags);
        LinkedHashSet<String> timeHints = new LinkedHashSet<>();
        LinkedHashSet<String> metrics = new LinkedHashSet<>();
        LinkedHashSet<String> dimensions = semanticTerms;
        LinkedHashSet<String> filters = new LinkedHashSet<>();

        IntentDecision intentDecision = classifyIntent(tokenList);
        QueryIntent intent = intentDecision.intent();
        boolean requiresAggregation = intent == QueryIntent.AGGREGATION || intent == QueryIntent.TREND;
        boolean requiresJoin = intent == QueryIntent.DETAIL || intent == QueryIntent.COMPARISON || intent == QueryIntent.TREND;

        double confidence = confidenceFromSignals(intent, intentDecision.probability(), posAnalysis);

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

    private QueryUnderstanding unavailableUnderstanding(String question) {
        return new QueryUnderstanding(
                QueryIntent.UNKNOWN,
                null,
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                false,
                false,
                0.20
        );
    }

    private boolean isSemanticTag(String tag) {
        if (tag == null || tag.isBlank()) return false;
        String normalizedTag = tag.toUpperCase(Locale.ROOT);
        return normalizedTag.contains("NOUN")
                || normalizedTag.contains("PROPN")
                || normalizedTag.contains("ADJ")
                || normalizedTag.contains("NUM");
    }

    private LinkedHashSet<String> extractSemanticTerms(List<String> tokens, String[] tags) {
        LinkedHashSet<String> terms = new LinkedHashSet<>();
        for (int i = 0; i < tokens.size(); i++) {
            String tag = i < tags.length ? tags[i] : "";
            if (isSemanticTag(tag)) {
                terms.add(tokens.get(i));
            }
        }
        return terms;
    }

    private double confidenceFromSignals(
            QueryIntent intent,
            double intentProbability,
            PosAnalysis posAnalysis
    ) {
        List<String> tokens = posAnalysis.tokens();
        String[] tags = posAnalysis.tags();
        if (tokens == null || tokens.isEmpty()) {
            return 0.20;
        }
        int contentCount = 0;
        for (String tag : tags) {
            if (isSemanticTag(tag)) {
                contentCount++;
            }
        }
        double contentRatio = (double) contentCount / (double) tokens.size();
        double avgTagConfidence = posAnalysis.averageProbability();
        double confidence = intent == QueryIntent.UNKNOWN ? 0.30 : 0.55;
        confidence += Math.min(0.20, intentProbability * 0.20);
        confidence += Math.min(0.20, contentRatio * 0.20);
        confidence += Math.min(0.15, avgTagConfidence * 0.15);
        return Math.max(0.0, Math.min(confidence, 0.92));
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

    private PosAnalysis analyzePos(List<String> tokenList) {
        Sequence bestSequence = posTagger.topKSequences(tokenList.toArray(String[]::new))[0];
        String[] tags = bestSequence.getOutcomes().stream().map(String::valueOf).toArray(String[]::new);
        double[] probabilities = bestSequence.getProbs();
        double avgProbability = 0.0;
        if (probabilities != null && probabilities.length > 0) {
            double sum = 0.0;
            for (double probability : probabilities) {
                sum += probability;
            }
            avgProbability = sum / (double) probabilities.length;
        }
        return new PosAnalysis(tokenList, tags, avgProbability);
    }

    private IntentDecision classifyIntent(List<String> tokenList) {
        double[] outcomes = intentCategorizer.categorize(tokenList.toArray(String[]::new));
        String bestCategory = intentCategorizer.getBestCategory(outcomes);
        double probability = 0.0;
        if (outcomes != null && outcomes.length > 0) {
            int bestIndex = intentCategorizer.getIndex(bestCategory);
            if (bestIndex >= 0 && bestIndex < outcomes.length) {
                probability = outcomes[bestIndex];
            }
        }
        return new IntentDecision(toQueryIntent(bestCategory), probability);
    }

    private QueryIntent toQueryIntent(String category) {
        if (category == null || category.isBlank()) {
            return QueryIntent.UNKNOWN;
        }
        try {
            return QueryIntent.valueOf(category.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            return QueryIntent.UNKNOWN;
        }
    }

    private record IntentDecision(QueryIntent intent, double probability) {}
    private record PosAnalysis(List<String> tokens, String[] tags, double averageProbability) {}

}


