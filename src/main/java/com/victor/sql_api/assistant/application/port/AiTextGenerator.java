package com.victor.sql_api.assistant.application.port;

public interface AiTextGenerator {
    String provider();

    String generate(String systemPrompt, String userPrompt);
}
