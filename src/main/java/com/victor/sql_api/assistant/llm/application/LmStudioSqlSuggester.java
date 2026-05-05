package com.victor.sql_api.assistant.llm.application;

import com.victor.sql_api.assistant.llm.provider.LmStudioTextGenerator;
import com.victor.sql_api.assistant.nl2sql.application.Nl2SqlOrchestrator;
import org.springframework.stereotype.Service;

@Service
public class LmStudioSqlSuggester extends BaseLlmSqlSuggester {
    public LmStudioSqlSuggester(
            Nl2SqlOrchestrator nl2SqlOrchestrator,
            LmStudioTextGenerator lmStudioTextGenerator
    ) {
        super(nl2SqlOrchestrator, lmStudioTextGenerator);
    }

    @Override
    public String provider() {
        return "lmstudio";
    }
}








