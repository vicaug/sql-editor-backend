package com.victor.sql_api.assistant.infrastructure.llm;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.ai.lmstudio")
public record LmStudioProperties(
        String baseUrl,
        String model,
        Double temperature,
        Integer timeoutSeconds
) {
    public LmStudioProperties {
        if (baseUrl == null || baseUrl.isBlank()) {
            baseUrl = "http://localhost:1234";
        }
        if (model == null || model.isBlank()) {
            model = "local-model";
        }
        if (temperature == null) {
            temperature = 0.0;
        }
        if (timeoutSeconds == null || timeoutSeconds <= 0) {
            timeoutSeconds = 25;
        }
    }
}


