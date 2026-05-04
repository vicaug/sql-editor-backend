package com.victor.sql_api.assistant.application.service;

import com.victor.sql_api.assistant.infrastructure.llm.OpenAiTextGenerator;
import com.victor.sql_api.assistant.nl2sql.application.service.SqlGuardService;
import com.victor.sql_api.assistant.context.application.router.MetadataContextProviderRouter;
import org.springframework.stereotype.Service;

@Service
public class AiAssistantService extends BaseAiSuggestionService {
    public AiAssistantService(
            MetadataContextProviderRouter metadataContextRouterService,
            OpenAiTextGenerator openAiTextGenerator,
            SqlGuardService sqlGuardService
    ) {
        super(metadataContextRouterService, openAiTextGenerator, sqlGuardService);
    }

    @Override
    public String provider() {
        return "openai";
    }
}



