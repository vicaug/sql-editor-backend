package com.victor.sql_api.assistant.application.service;

import com.victor.sql_api.assistant.infrastructure.llm.LmStudioTextGenerator;
import com.victor.sql_api.assistant.nl2sql.application.service.SqlGuardService;
import com.victor.sql_api.assistant.context.application.router.MetadataContextProviderRouter;
import org.springframework.stereotype.Service;

@Service
public class AiAssistantLocalService extends BaseAiSuggestionService {
    public AiAssistantLocalService(
            MetadataContextProviderRouter metadataContextRouterService,
            LmStudioTextGenerator lmStudioTextGenerator,
            SqlGuardService sqlGuardService
    ) {
        super(metadataContextRouterService, lmStudioTextGenerator, sqlGuardService);
    }

    @Override
    public String provider() {
        return "lmstudio";
    }
}



