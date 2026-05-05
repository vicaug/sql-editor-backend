package com.victor.sql_api.assistant.llm.application;

import com.victor.sql_api.assistant.nl2sql.model.AiAssistantResult;

public interface LlmSqlSuggester {
    String provider();

    AiAssistantResult suggest(
            String prompt,
            String currentSql,
            boolean enableSqlGuard,
            String metadataProvider,
            String queryUnderstandingEngine
    );
}








