package com.victor.sql_api.assistant.metadata_retrieval.application;

import com.victor.sql_api.assistant.nl2sql.model.QueryIntent;
import com.victor.sql_api.assistant.nl2sql.model.QueryUnderstanding;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.util.Sequence;
import opennlp.tools.util.Span;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Order(Ordered.HIGHEST_PRECEDENCE)
public class OpenNlpQueryUnderstandingEngine implements QueryUnderstandingEngine {
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
        if (posTagger == null) {
            return unavailableUnderstanding(question);
        }
        List<String> tokenList = tokenize(question);
        if (tokenList.isEmpty()) {
            return unavailableUnderstanding(question);
        }

        PosAnalysis posAnalysis = analyzePos(tokenList);
        LinkedHashSet<String> semanticTerms = new LinkedHashSet<>(tokenList);
        LinkedHashSet<String> timeHints = new LinkedHashSet<>();
        LinkedHashSet<String> metrics = new LinkedHashSet<>();
        LinkedHashSet<String> dimensions = semanticTerms;
        LinkedHashSet<String> filters = new LinkedHashSet<>();

        QueryIntent intent = null;
        Boolean requiresAggregation = null;
        Boolean requiresJoin = null;

        double confidence = posAnalysis.averageProbability();

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
                null,
                null,
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                null,
                null,
                Double.NaN
        );
    }

    private List<String> tokenize(String text) {
        if (text == null || text.isBlank()) return List.of();
        Span[] spans = tokenizer.tokenizePos(text);
        List<String> tokens = new ArrayList<>(spans.length);
        for (Span span : spans) {
            String token = text.substring(span.getStart(), span.getEnd()).trim();
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

    private record PosAnalysis(List<String> tokens, String[] tags, double averageProbability) {}

}
