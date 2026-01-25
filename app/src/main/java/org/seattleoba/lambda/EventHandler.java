package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import org.seattleoba.lambda.dagger.DaggerLambdaComponent;
import org.seattleoba.lambda.dagger.LambdaComponent;

public class EventHandler implements RequestHandler<DynamodbEvent, Void> {
    private final LambdaComponent lambdaComponent = DaggerLambdaComponent.create();

    @Override
    public Void handleRequest(final DynamodbEvent input, final Context context) {
        return lambdaComponent.eventHandler().handleRequest(input, context);
    }
}
