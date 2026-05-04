package com.victor.sql_api.assistant.nl2sql.application.service;

import com.victor.sql_api.assistant.nl2sql.domain.model.QueryIntent;
import com.victor.sql_api.assistant.nl2sql.domain.model.QueryUnderstanding;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class QueryUnderstandingServiceTest {

    private final QueryUnderstandingService service = new QueryUnderstandingService(
            java.util.List.of(new HeuristicQueryUnderstandingEngine())
    );

    @Test
    void shouldDetectGenericAggregationAndDimensions() {
        QueryUnderstanding result = service.analyze("Qual o total de pedidos por categoria e canal em 2024?");

        assertEquals(QueryIntent.AGGREGATION, result.intent());
        assertTrue(result.requiresAggregation());
        assertTrue(result.requiresJoin());
        assertTrue(result.dimensions().contains("categoria"));
        assertTrue(result.dimensions().contains("canal"));
        assertTrue(result.dimensions().contains("agrupamento"));
        assertTrue(result.filters().contains("year=2024"));
        assertTrue(result.timeHints().contains("2024"));
    }

    @Test
    void shouldStopDimensionCaptureAtClauseBoundary() {
        QueryUnderstanding result = service.analyze("Listar pedidos por cliente com status pago");

        assertEquals(QueryIntent.DETAIL, result.intent());
        assertTrue(result.dimensions().contains("cliente"));
        assertFalse(result.dimensions().contains("cliente com status pago"));
    }

    @Test
    void shouldNotClassifyDomainWordAsGenericMetric() {
        QueryUnderstanding result = service.analyze("faturamento por mes");

        assertFalse(result.metrics().contains("faturamento"));
    }
}
