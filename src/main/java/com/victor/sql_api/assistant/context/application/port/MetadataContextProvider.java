package com.victor.sql_api.assistant.context.application.port;

import com.victor.sql_api.assistant.retrieval.model.application.RetrievalRequest;
import com.victor.sql_api.assistant.nl2sql.domain.model.Nl2SqlContext;

public interface MetadataContextProvider {
    String provider();

    Nl2SqlContext buildContext(RetrievalRequest request);
}



