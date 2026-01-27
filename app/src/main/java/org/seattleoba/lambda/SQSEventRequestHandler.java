package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.seattleoba.lambda.dagger.BevyTicketSQSEventRequestHandlerComponent;
import org.seattleoba.lambda.dagger.DaggerBevyTicketSQSEventRequestHandlerComponent;

public class SQSEventRequestHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
    private final BevyTicketSQSEventRequestHandlerComponent lambdaComponent = DaggerBevyTicketSQSEventRequestHandlerComponent.create();

    @Override
    public SQSBatchResponse handleRequest(final SQSEvent sqsEvent, final Context context) {
        return lambdaComponent.requestHandler().handleRequest(sqsEvent, context);
    }
}
