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
    private static final String OPENNLP_ENGINE = "opennlp";
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
        if (openNlp != null && openNlp.isAvailable()) {
            QueryUnderstanding openNlpResult = openNlp.analyze(question);
            if (openNlpResult != null) {
                return new QueryUnderstandingDecision(openNlpResult, OPENNLP_ENGINE, false, null);
            }
            log.warn("OpenNLP nao retornou resultado. Retornando UNKNOWN.");
            return buildUnknownDecision("opennlp_failed");
        }

        log.warn("Nenhum QueryUnderstandingEngine disponivel. Retornando UNKNOWN.");
        return buildUnknownDecision("opennlp_unavailable");
    }

    private QueryUnderstandingEngine findEngine(String name) {
        for (QueryUnderstandingEngine engine : engines) {
            if (engine.name() != null && engine.name().equalsIgnoreCase(name)) {
                return engine;
            }
        }
        return null;
    }

    private QueryUnderstandingDecision buildUnknownDecision(String reason) {
        QueryUnderstanding unknown = new QueryUnderstanding(
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
        return new QueryUnderstandingDecision(unknown, "none", true, reason);
    }
}










