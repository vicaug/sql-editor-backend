package com.victor.sql_api.assistant.metadata_retrieval.provider;

import com.victor.sql_api.assistant.metadata_retrieval.model.RetrievalRequest;
import com.victor.sql_api.assistant.nl2sql.model.Nl2SqlContext;

public interface MetadataContextProvider {
    String provider();

    Nl2SqlContext buildContext(RetrievalRequest request);
}








