package com.victor.sql_api.assistant.llm.application;

import com.victor.sql_api.assistant.nl2sql.model.AiAssistantResult;
import com.victor.sql_api.assistant.llm.provider.AiTextGenerator;
import com.victor.sql_api.assistant.nl2sql.application.Nl2SqlOrchestrator;

abstract class BaseLlmSqlSuggester implements LlmSqlSuggester {
    private final Nl2SqlOrchestrator nl2SqlOrchestrator;
    private final AiTextGenerator aiTextGenerator;

    protected BaseLlmSqlSuggester(
            Nl2SqlOrchestrator nl2SqlOrchestrator,
            AiTextGenerator aiTextGenerator
    ) {
        this.nl2SqlOrchestrator = nl2SqlOrchestrator;
        this.aiTextGenerator = aiTextGenerator;
    }

    @Override
    public AiAssistantResult suggest(
            String prompt,
            String currentSql,
            boolean enableSqlGuard,
            String metadataProvider,
            String queryUnderstandingEngine
    ) {
        return nl2SqlOrchestrator.suggest(
                prompt,
                currentSql,
                enableSqlGuard,
                metadataProvider,
                queryUnderstandingEngine,
                aiTextGenerator
        );
    }
}








