package com.victor.sql_api.assistant.context.application.router;

import com.victor.sql_api.assistant.context.application.port.MetadataContextProvider;
import com.victor.sql_api.assistant.retrieval.model.application.RetrievalRequest;
import com.victor.sql_api.assistant.nl2sql.domain.model.Nl2SqlContext;
import com.victor.sql_api.shared.exception.BadRequestException;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Service
public class MetadataContextProviderRouter {
    private static final String DEFAULT_PROVIDER = "askdata_like";

    private final Map<String, MetadataContextProvider> providersByName;

    public MetadataContextProviderRouter(List<MetadataContextProvider> providers) {
        this.providersByName = new HashMap<>();
        for (MetadataContextProvider provider : providers) {
            providersByName.put(normalizeProvider(provider.provider()), provider);
        }
    }

    public Nl2SqlContext buildContext(RetrievalRequest request, String provider) {
        MetadataContextProvider selectedProvider = resolveProvider(provider);
        return selectedProvider.buildContext(request);
    }

    private MetadataContextProvider resolveProvider(String provider) {
        String normalizedProvider = normalizeProvider(provider);
        MetadataContextProvider selectedProvider = providersByName.get(normalizedProvider);
        if (selectedProvider != null) {
            return selectedProvider;
        }

        MetadataContextProvider defaultProvider = providersByName.get(DEFAULT_PROVIDER);
        if (defaultProvider != null) {
            return defaultProvider;
        }

        throw new BadRequestException("METADATA_PROVIDER_UNAVAILABLE", "Nenhum provedor de metadata configurado no backend.");
    }

    private String normalizeProvider(String provider) {
        if (provider == null || provider.trim().isBlank()) {
            return DEFAULT_PROVIDER;
        }
        return provider.trim().toLowerCase(Locale.ROOT);
    }
}



