package com.victor.sql_api.assistant.retrieval.model.domain;

import java.util.List;

public record MetadataContext(
        String question,
        List<RelevantTable> tables,
        List<RelevantColumn> columns,
        List<RelevantRelationship> relationships,
        List<RetrievalHint> hints,
        RetrievalDiagnostics diagnostics
) {
}


