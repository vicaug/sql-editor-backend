package com.victor.sql_api.assistant.llm.application;

import com.victor.sql_api.assistant.llm.provider.OpenAiTextGenerator;
import com.victor.sql_api.assistant.nl2sql.application.Nl2SqlOrchestrator;
import org.springframework.stereotype.Service;

@Service
public class OpenAiSqlSuggester extends BaseLlmSqlSuggester {
    public OpenAiSqlSuggester(
            Nl2SqlOrchestrator nl2SqlOrchestrator,
            OpenAiTextGenerator openAiTextGenerator
    ) {
        super(nl2SqlOrchestrator, openAiTextGenerator);
    }

    @Override
    public String provider() {
        return "openai";
    }
}








