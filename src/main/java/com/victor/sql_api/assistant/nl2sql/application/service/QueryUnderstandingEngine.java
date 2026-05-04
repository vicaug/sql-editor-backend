package com.victor.sql_api.assistant.nl2sql.application.service;

import com.victor.sql_api.assistant.nl2sql.domain.model.QueryUnderstanding;

public interface QueryUnderstandingEngine {
    String name();
    boolean isAvailable();
    QueryUnderstanding analyze(String question);
}



