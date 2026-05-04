package com.victor.sql_api.assistant.application.service;

import com.victor.sql_api.assistant.application.model.AiAssistantResult;
import com.victor.sql_api.shared.exception.BadRequestException;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Service
public class AiAssistantRouterService {
    private static final String DEFAULT_PROVIDER = "lmstudio";

    private final Map<String, AiSuggestionService> servicesByProvider;

    public AiAssistantRouterService(List<AiSuggestionService> services) {
        this.servicesByProvider = new HashMap<>();
        for (AiSuggestionService service : services) {
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
        AiSuggestionService service = resolveProvider(provider);
        return service.suggest(prompt, currentSql, enableSqlGuard, metadataProvider, queryUnderstandingEngine);
    }

    private AiSuggestionService resolveProvider(String provider) {
        String normalizedProvider = normalizeProvider(provider);
        AiSuggestionService service = servicesByProvider.get(normalizedProvider);
        if (service != null) {
            return service;
        }

        AiSuggestionService defaultService = servicesByProvider.get(DEFAULT_PROVIDER);
        if (defaultService != null) {
            return defaultService;
        }

        throw new BadRequestException("AI_PROVIDER_UNAVAILABLE", "Nenhum provedor de IA configurado no backend.");
    }

    private String normalizeProvider(String provider) {
        if (provider == null || provider.trim().isBlank()) {
            return DEFAULT_PROVIDER;
        }
        String normalized = provider.trim().toLowerCase(Locale.ROOT);
        if ("ollama".equals(normalized)) {
            return "lmstudio";
        }
        return normalized;
    }
}
