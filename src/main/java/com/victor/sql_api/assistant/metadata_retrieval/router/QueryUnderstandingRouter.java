package com.victor.sql_api.assistant.metadata_retrieval.router;

import com.victor.sql_api.assistant.metadata_retrieval.application.QueryUnderstandingEngine;
import com.victor.sql_api.assistant.nl2sql.model.QueryIntent;
import com.victor.sql_api.assistant.nl2sql.model.QueryUnderstandingDecision;
import com.victor.sql_api.assistant.nl2sql.model.QueryUnderstanding;
import com.victor.sql_api.shared.exception.BadRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class QueryUnderstandingRouter {
    private static final Logger log = LoggerFactory.getLogger(QueryUnderstandingRouter.class);
    private static final double OPENNLP_CONFIDENCE_THRESHOLD = 0.55;
    private static final String OPENNLP_ENGINE = "opennlp";
    private static final String HEURISTIC_ENGINE = "heuristic";
    private final List<QueryUnderstandingEngine> engines;

    public QueryUnderstandingRouter(List<QueryUnderstandingEngine> engines) {
        AnnotationAwareOrderComparator.sort(engines);
        this.engines = List.copyOf(engines);
    }

    public QueryUnderstanding analyze(String question) {
        return analyzeDecision(question, null).understanding();
    }

    public QueryUnderstandingDecision analyzeDecision(String question) {
        return analyzeDecision(question, null);
    }

    public QueryUnderstandingDecision analyzeDecision(String question, String preferredEngine) {
        String preferred = preferredEngine == null ? "" : preferredEngine.trim().toLowerCase();
        if (!preferred.isBlank() && !preferred.equals("auto")) {
            QueryUnderstandingEngine forcedEngine = findEngine(preferred);
            if (forcedEngine == null) {
                throw new BadRequestException("QUERY_UNDERSTANDING_ENGINE_INVALID", "Engine de entendimento invalido: " + preferred);
            }
            if (!forcedEngine.isAvailable()) {
                throw new BadRequestException(
                        "QUERY_UNDERSTANDING_ENGINE_UNAVAILABLE",
                        "Engine de entendimento indisponivel: " + preferred
                );
            }
            QueryUnderstanding forcedResult = forcedEngine.analyze(question);
            if (forcedResult == null) {
                throw new BadRequestException(
                        "QUERY_UNDERSTANDING_ENGINE_FAILED",
                        "Engine de entendimento nao retornou resultado: " + preferred
                );
            }
            return new QueryUnderstandingDecision(forcedResult, forcedEngine.name(), false, "forced_by_request");
        }

        QueryUnderstandingEngine openNlp = findEngine(OPENNLP_ENGINE);
        QueryUnderstandingEngine heuristic = findEngine(HEURISTIC_ENGINE);

        if (openNlp != null && openNlp.isAvailable()) {
            QueryUnderstanding openNlpResult = openNlp.analyze(question);
            if (openNlpResult != null && openNlpResult.confidence() >= OPENNLP_CONFIDENCE_THRESHOLD) {
                return new QueryUnderstandingDecision(openNlpResult, OPENNLP_ENGINE, false, null);
            }
            if (heuristic != null && heuristic.isAvailable()) {
                QueryUnderstanding heuristicResult = heuristic.analyze(question);
                if (heuristicResult != null) {
                    return new QueryUnderstandingDecision(
                            heuristicResult,
                            HEURISTIC_ENGINE,
                            true,
                            "opennlp_low_confidence"
                    );
                }
            }
            if (openNlpResult != null) {
                return new QueryUnderstandingDecision(openNlpResult, OPENNLP_ENGINE, false, "opennlp_only_available");
            }
        }

        if (heuristic != null && heuristic.isAvailable()) {
            QueryUnderstanding heuristicResult = heuristic.analyze(question);
            if (heuristicResult != null) {
                return new QueryUnderstandingDecision(
                        heuristicResult,
                        HEURISTIC_ENGINE,
                        true,
                        "opennlp_unavailable"
                );
            }
        }

        for (QueryUnderstandingEngine engine : engines) {
            if (!engine.isAvailable()) {
                continue;
            }
            QueryUnderstanding result = engine.analyze(question);
            if (result != null) {
                return new QueryUnderstandingDecision(result, engine.name(), true, "generic_fallback_path");
            }
        }

        log.warn("Nenhum QueryUnderstandingEngine disponivel. Retornando UNKNOWN.");
        QueryUnderstanding unknown = new QueryUnderstanding(
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
        return new QueryUnderstandingDecision(unknown, "none", true, "no_engine_available");
    }

    private QueryUnderstandingEngine findEngine(String name) {
        for (QueryUnderstandingEngine engine : engines) {
            if (engine.name() != null && engine.name().equalsIgnoreCase(name)) {
                return engine;
            }
        }
        return null;
    }
}










