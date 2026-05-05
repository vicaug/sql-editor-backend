package com.victor.sql_api.assistant.llm.router;

import com.victor.sql_api.assistant.nl2sql.model.AiAssistantResult;
import com.victor.sql_api.assistant.llm.application.LlmSqlSuggester;
import com.victor.sql_api.shared.exception.BadRequestException;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Service
public class LlmProviderRouter {
    private static final String DEFAULT_PROVIDER = "lmstudio";

    private final Map<String, LlmSqlSuggester> servicesByProvider;

    public LlmProviderRouter(List<LlmSqlSuggester> services) {
        this.servicesByProvider = new HashMap<>();
        for (LlmSqlSuggester service : services) {
            servicesByProvider.put(normalizeProvider(service.provider()), service);
        }
    }

    public AiAssistantResult suggest(
            String prompt,
            String currentSql,
            String provider,
            boolean enableSqlGuard,
            String metadataProvider,
            String queryUnderstandingEngine
    ) {
        LlmSqlSuggester service = resolveProvider(provider);
        return service.suggest(prompt, currentSql, enableSqlGuard, metadataProvider, queryUnderstandingEngine);
    }

    private LlmSqlSuggester resolveProvider(String provider) {
        String normalizedProvider = normalizeProvider(provider);
        LlmSqlSuggester service = servicesByProvider.get(normalizedProvider);
        if (service != null) {
            return service;
        }

        LlmSqlSuggester defaultService = servicesByProvider.get(DEFAULT_PROVIDER);
        if (defaultService != null) {
            return defaultService;
        }

        throw new BadRequestException("AI_PROVIDER_UNAVAILABLE", "Nenhum provedor de IA configurado no backend.");
    }

    private String normalizeProvider(String provider) {
        if (provider == null || provider.trim().isBlank()) {
            return DEFAULT_PROVIDER;
        }
        return provider.trim().toLowerCase(Locale.ROOT);
    }
}








