package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.seattleoba.lambda.dagger.DaggerStripeEventSQSRequestHandlerComponent;
import org.seattleoba.lambda.dagger.StripeEventSQSRequestHandlerComponent;

public class StripeEventRequestHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
    private final StripeEventSQSRequestHandlerComponent lambdaComponent = DaggerStripeEventSQSRequestHandlerComponent.create();

    @Override
    public SQSBatchResponse handleRequest(final SQSEvent sqsEvent, final Context context) {
        return lambdaComponent.requestHandler().handleRequest(sqsEvent, context);
    }
}
