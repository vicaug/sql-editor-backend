package com.victor.sql_api.assistant.application.service;

import com.victor.sql_api.assistant.askdatalike.application.port.MetadataContextService;
import com.victor.sql_api.assistant.metadata.application.model.RetrievalRequest;
import com.victor.sql_api.assistant.nl2sql.domain.model.Nl2SqlContext;
import com.victor.sql_api.shared.exception.BadRequestException;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Service
public class MetadataContextRouterService {
    private static final String DEFAULT_PROVIDER = "askdata_like";

    private final Map<String, MetadataContextService> servicesByProvider;

    public MetadataContextRouterService(List<MetadataContextService> services) {
        this.servicesByProvider = new HashMap<>();
        for (MetadataContextService service : services) {
            servicesByProvider.put(normalizeProvider(service.provider()), service);
        }
    }

    public Nl2SqlContext buildContext(RetrievalRequest request, String provider) {
        MetadataContextService service = resolveProvider(provider);
        return service.buildContext(request);
    }

    private MetadataContextService resolveProvider(String provider) {
        String normalizedProvider = normalizeProvider(provider);
        MetadataContextService service = servicesByProvider.get(normalizedProvider);
        if (service != null) {
            return service;
        }

        MetadataContextService defaultService = servicesByProvider.get(DEFAULT_PROVIDER);
        if (defaultService != null) {
            return defaultService;
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

