package com.victor.sql_api.assistant.metadata_retrieval.application;

import com.victor.sql_api.assistant.nl2sql.model.QueryUnderstanding;

public interface QueryUnderstandingEngine {
    String name();
    boolean isAvailable();
    QueryUnderstanding analyze(String question);
}









