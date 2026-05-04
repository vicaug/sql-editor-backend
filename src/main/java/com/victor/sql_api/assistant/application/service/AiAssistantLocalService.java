package com.victor.sql_api.assistant.application.service;

import com.victor.sql_api.assistant.infrastructure.llm.OllamaTextGenerator;
import com.victor.sql_api.assistant.nl2sql.application.service.SqlGuardService;
import org.springframework.stereotype.Service;

@Service
public class AiAssistantLocalService extends BaseAiSuggestionService {
    public AiAssistantLocalService(
            MetadataContextRouterService metadataContextRouterService,
            OllamaTextGenerator ollamaTextGenerator,
            SqlGuardService sqlGuardService
    ) {
        super(metadataContextRouterService, ollamaTextGenerator, sqlGuardService);
    }

    @Override
    public String provider() {
        return "lmstudio";
    }
}
