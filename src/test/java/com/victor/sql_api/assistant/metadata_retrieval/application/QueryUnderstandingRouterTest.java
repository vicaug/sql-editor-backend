package com.victor.sql_api.assistant.metadata_retrieval.application;

import com.victor.sql_api.assistant.metadata_retrieval.router.QueryUnderstandingRouter;
import com.victor.sql_api.assistant.nl2sql.model.QueryIntent;
import com.victor.sql_api.assistant.nl2sql.model.QueryUnderstanding;
import com.victor.sql_api.assistant.nl2sql.model.QueryUnderstandingDecision;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryUnderstandingRouterTest {

    @Test
    void shouldUseOpenNlpInAutoMode() {
        QueryUnderstandingRouter router = new QueryUnderstandingRouter(List.of(new StubOpenNlpEngine(true)));
        QueryUnderstandingDecision decision = router.analyzeDecision("Qual o total de pedidos?");

        assertEquals("opennlp", decision.selectedEngine());
        assertFalse(decision.fallbackApplied());
        assertNull(decision.fallbackReason());
        assertEquals(QueryIntent.AGGREGATION, decision.understanding().intent());
    }

    @Test
    void shouldSupportForcedOpenNlp() {
        QueryUnderstandingRouter router = new QueryUnderstandingRouter(List.of(new StubOpenNlpEngine(true)));
        QueryUnderstandingDecision decision = router.analyzeDecision("Qual o total de pedidos?", "opennlp");

        assertEquals("opennlp", decision.selectedEngine());
        assertFalse(decision.fallbackApplied());
        assertEquals("forced_by_request", decision.fallbackReason());
    }

    @Test
    void shouldReturnUnknownWhenOpenNlpIsUnavailable() {
        QueryUnderstandingRouter router = new QueryUnderstandingRouter(List.of(new StubOpenNlpEngine(false)));
        QueryUnderstandingDecision decision = router.analyzeDecision("qualquer pergunta");

        assertEquals(QueryIntent.UNKNOWN, decision.understanding().intent());
        assertEquals("none", decision.selectedEngine());
        assertTrue(decision.fallbackApplied());
        assertEquals("opennlp_unavailable", decision.fallbackReason());
    }

    private static final class StubOpenNlpEngine implements QueryUnderstandingEngine {
        private final boolean available;

        private StubOpenNlpEngine(boolean available) {
            this.available = available;
        }

        @Override
        public String name() {
            return "opennlp";
        }

        @Override
        public boolean isAvailable() {
            return available;
        }

        @Override
        public QueryUnderstanding analyze(String question) {
            return new QueryUnderstanding(
                    QueryIntent.AGGREGATION,
                    null,
                    List.of("total"),
                    List.of("categoria", "canal", "agrupamento"),
                    List.of("year=2024"),
                    List.of("2024"),
                    true,
                    true,
                    0.80
            );
        }
    }
}

