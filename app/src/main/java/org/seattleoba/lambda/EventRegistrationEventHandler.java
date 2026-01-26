package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import org.seattleoba.lambda.dagger.DaggerEventHandlerLambdaComponent;
import org.seattleoba.lambda.dagger.EventHandlerLambdaComponent;

public class EventRegistrationEventHandler implements RequestHandler<DynamodbEvent, Void> {
    private final EventHandlerLambdaComponent lambdaComponent = DaggerEventHandlerLambdaComponent.create();

    @Override
    public Void handleRequest(final DynamodbEvent input, final Context context) {
        return lambdaComponent.eventHandler().handleRequest(input, context);
    }
}
