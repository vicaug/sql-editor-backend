package com.victor.sql_api.assistant.askdatalike.application.service;

import com.victor.sql_api.assistant.askdatalike.application.port.MetadataContextService;
import com.victor.sql_api.assistant.metadata.application.model.RetrievalRequest;
import com.victor.sql_api.assistant.metadata.domain.model.MetadataContext;
import com.victor.sql_api.assistant.nl2sql.application.service.PromptContextBuilder;
import com.victor.sql_api.assistant.nl2sql.application.service.QueryUnderstandingService;
import com.victor.sql_api.assistant.nl2sql.domain.model.Nl2SqlContext;
import com.victor.sql_api.assistant.nl2sql.domain.model.QueryUnderstandingDecision;
import com.victor.sql_api.assistant.nl2sql.domain.model.QueryUnderstanding;
import org.springframework.stereotype.Service;

@Service
public class DefaultAskDataLikeContextService implements MetadataContextService {

    private final QueryUnderstandingService queryUnderstandingService;
    private final AskDataLikeMetadataRetrievalService metadataRetrievalService;
    private final PromptContextBuilder promptContextBuilder;

    public DefaultAskDataLikeContextService(
            QueryUnderstandingService queryUnderstandingService,
            AskDataLikeMetadataRetrievalService metadataRetrievalService,
            PromptContextBuilder promptContextBuilder
    ) {
        this.queryUnderstandingService = queryUnderstandingService;
        this.metadataRetrievalService = metadataRetrievalService;
        this.promptContextBuilder = promptContextBuilder;
    }

    @Override
    public String provider() {
        return "askdata_like";
    }

    @Override
    public Nl2SqlContext buildContext(RetrievalRequest request) {
        QueryUnderstandingDecision decision = queryUnderstandingService.analyzeDecision(
                request.question(),
                request.queryUnderstandingEngine()
        );
        QueryUnderstanding understanding = decision.understanding();
        MetadataContext metadataContext = metadataRetrievalService.retrieve(
                request,
                understanding,
                decision.selectedEngine(),
                decision.fallbackApplied(),
                decision.fallbackReason()
        );
        String promptContext = promptContextBuilder.build(request.question(), understanding, metadataContext);
        return new Nl2SqlContext(
                request.question(),
                understanding,
                metadataContext,
                decision.selectedEngine(),
                understanding == null ? 0.0 : understanding.confidence(),
                decision.fallbackApplied(),
                decision.fallbackReason(),
                promptContext
        );
    }
}
