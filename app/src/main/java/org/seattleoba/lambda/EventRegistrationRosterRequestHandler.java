package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.seattleoba.lambda.dagger.DaggerEventRegistrationRosterRequestHandlerComponent;
import org.seattleoba.lambda.dagger.EventRegistrationRosterRequestHandlerComponent;
import org.seattleoba.lambda.model.EventRegistrationRosterRequest;
import org.seattleoba.lambda.model.EventRegistrationRosterResult;

public class EventRegistrationRosterRequestHandler implements
        RequestHandler<EventRegistrationRosterRequest, EventRegistrationRosterResult> {
    private final EventRegistrationRosterRequestHandlerComponent lambdaComponent =
            DaggerEventRegistrationRosterRequestHandlerComponent.create();

    @Override
    public EventRegistrationRosterResult handleRequest(
            final EventRegistrationRosterRequest input,
            final Context context) {
        return lambdaComponent.requestHandler().handleRequest(input, context);
    }
}
