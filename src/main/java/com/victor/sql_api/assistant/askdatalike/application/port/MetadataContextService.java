package com.victor.sql_api.assistant.askdatalike.application.port;

import com.victor.sql_api.assistant.metadata.application.model.RetrievalRequest;
import com.victor.sql_api.assistant.nl2sql.domain.model.Nl2SqlContext;

public interface MetadataContextService {
    String provider();

    Nl2SqlContext buildContext(RetrievalRequest request);
}

