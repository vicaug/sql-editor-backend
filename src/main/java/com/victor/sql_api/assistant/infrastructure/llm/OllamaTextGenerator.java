package com.victor.sql_api.assistant.infrastructure.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.victor.sql_api.assistant.application.port.AiTextGenerator;
import com.victor.sql_api.shared.exception.BadRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.http.client.SimpleClientHttpRequestFactory;

import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Map;

@Component
@EnableConfigurationProperties(OllamaProperties.class)
public class OllamaTextGenerator implements AiTextGenerator {
    private static final Logger log = LoggerFactory.getLogger(OllamaTextGenerator.class);

    private final RestClient restClient;
    private final ObjectMapper objectMapper;
    private final OllamaProperties properties;

    public OllamaTextGenerator(ObjectMapper objectMapper, OllamaProperties properties) {
        this.objectMapper = objectMapper;
        this.properties = properties;
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofSeconds(5));
        requestFactory.setReadTimeout(Duration.ofSeconds(properties.timeoutSeconds()));
        this.restClient = RestClient.builder()
                .baseUrl(properties.baseUrl())
                .requestFactory(requestFactory)
                .build();
    }

    @Override
    public String provider() {
        return "lmstudio";
    }

    @Override
    public String generate(String systemPrompt, String userPrompt) {
        long startedAt = System.nanoTime();
        try {
            String payload = restClient.post()
                    .uri("/v1/chat/completions")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of(
                            "model", properties.model(),
                            "messages", java.util.List.of(
                                    Map.of("role", "system", "content", systemPrompt),
                                    Map.of("role", "user", "content", userPrompt)
                            ),
                            "temperature", properties.temperature(),
                            "stream", false
                    ))
                    .retrieve()
                    .body(String.class);

            JsonNode root = objectMapper.readTree(payload == null ? "{}" : payload);
            long elapsedMs = (System.nanoTime() - startedAt) / 1_000_000;
            log.info("LM Studio response received in {} ms (model={})", elapsedMs, properties.model());
            return root.path("choices").path(0).path("message").path("content").asText("");
        } catch (ResourceAccessException ex) {
            long elapsedMs = (System.nanoTime() - startedAt) / 1_000_000;
            Throwable cause = ex.getCause();
            if (cause instanceof SocketTimeoutException) {
                log.warn("LM Studio timeout after {} ms (model={}, timeoutSeconds={})",
                        elapsedMs, properties.model(), properties.timeoutSeconds());
                throw new BadRequestException(
                        "AI_LMSTUDIO_TIMEOUT",
                        "Timeout ao chamar LM Studio. O backend conseguiu processar a requisicao, mas a LLM demorou alem do limite configurado."
                );
            }
            log.warn("LM Studio connection error after {} ms: {}", elapsedMs, ex.getMessage());
            throw new BadRequestException(
                    "AI_LMSTUDIO_CONNECTION_ERROR",
                    "Falha de conexao com LM Studio. Verifique se o servidor local esta ativo em " + properties.baseUrl() + "."
            );
        } catch (Exception ex) {
            long elapsedMs = (System.nanoTime() - startedAt) / 1_000_000;
            log.warn("LM Studio unexpected error after {} ms: {}", elapsedMs, ex.getMessage());
            throw new BadRequestException(
                    "AI_LMSTUDIO_UNAVAILABLE",
                    "Nao foi possivel gerar resposta no LM Studio local. Verifique se o servidor esta ativo e o modelo carregado."
            );
        }
    }
}
