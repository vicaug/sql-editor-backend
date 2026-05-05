package com.victor.sql_api.assistant.llm.provider;

public interface AiTextGenerator {
    String provider();

    String generate(String systemPrompt, String userPrompt);
}







