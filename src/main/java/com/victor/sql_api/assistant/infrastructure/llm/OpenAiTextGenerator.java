package com.victor.sql_api.assistant.infrastructure.llm;

import com.victor.sql_api.assistant.application.port.AiTextGenerator;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Component;

@Component
public class OpenAiTextGenerator implements AiTextGenerator {

    private final ChatClient chatClient;

    public OpenAiTextGenerator(ChatClient.Builder chatClientBuilder) {
        this.chatClient = chatClientBuilder.build();
    }

    @Override
    public String provider() {
        return "openai";
    }

    @Override
    public String generate(String systemPrompt, String userPrompt) {
        return chatClient.prompt()
                .system(systemPrompt)
                .user(userPrompt)
                .call()
                .content();
    }
}
