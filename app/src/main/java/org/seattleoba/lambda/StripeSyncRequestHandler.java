package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.seattleoba.lambda.dagger.DaggerStripeDataSyncRequestHandlerComponent;
import org.seattleoba.lambda.dagger.StripeDataSyncRequestHandlerComponent;
import org.seattleoba.lambda.model.StripeDataSyncRequest;

public class StripeSyncRequestHandler implements RequestHandler<StripeDataSyncRequest, Void> {
    private final StripeDataSyncRequestHandlerComponent stripeDataSyncRequestHandlerComponent = DaggerStripeDataSyncRequestHandlerComponent.create();

    @Override
    public Void handleRequest(final StripeDataSyncRequest input, final Context context) {
        return stripeDataSyncRequestHandlerComponent.requestHandler().handleRequest(input, context);
    }
}
