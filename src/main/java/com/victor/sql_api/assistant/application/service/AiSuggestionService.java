package com.victor.sql_api.assistant.application.service;

import com.victor.sql_api.assistant.application.model.AiAssistantResult;

public interface AiSuggestionService {
    String provider();

    AiAssistantResult suggest(
            String prompt,
            String currentSql,
            boolean enableSqlGuard,
            String metadataProvider,
            String queryUnderstandingEngine
    );
}
