package com.victor.sql_api.assistant.metadata_retrieval.provider;

import com.victor.sql_api.assistant.metadata_retrieval.application.AskDataLikeMetadataRetrievalService;
import com.victor.sql_api.assistant.metadata_retrieval.model.RetrievalRequest;
import com.victor.sql_api.assistant.metadata.model.context.MetadataContext;
import com.victor.sql_api.assistant.metadata_retrieval.application.PromptContextBuilder;
import com.victor.sql_api.assistant.metadata_retrieval.router.QueryUnderstandingRouter;
import com.victor.sql_api.assistant.nl2sql.model.Nl2SqlContext;
import com.victor.sql_api.assistant.nl2sql.model.QueryUnderstanding;
import com.victor.sql_api.assistant.nl2sql.model.QueryUnderstandingDecision;
import org.springframework.stereotype.Service;

@Service
public class AskDataLikeContextProvider implements MetadataContextProvider {

    private final QueryUnderstandingRouter queryUnderstandingRouter;
    private final AskDataLikeMetadataRetrievalService metadataRetrievalService;
    private final PromptContextBuilder promptContextBuilder;

    public AskDataLikeContextProvider(
            QueryUnderstandingRouter queryUnderstandingRouter,
            AskDataLikeMetadataRetrievalService metadataRetrievalService,
            PromptContextBuilder promptContextBuilder
    ) {
        this.queryUnderstandingRouter = queryUnderstandingRouter;
        this.metadataRetrievalService = metadataRetrievalService;
        this.promptContextBuilder = promptContextBuilder;
    }

    @Override
    public String provider() {
        return "askdata_like";
    }

    @Override
    public Nl2SqlContext buildContext(RetrievalRequest request) {
        QueryUnderstandingDecision decision = queryUnderstandingRouter.analyzeDecision(
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








