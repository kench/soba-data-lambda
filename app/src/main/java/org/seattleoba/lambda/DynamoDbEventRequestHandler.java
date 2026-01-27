package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;
import org.seattleoba.lambda.dagger.BevyTicketDynamodbEventHandlerComponent;
import org.seattleoba.lambda.dagger.DaggerBevyTicketDynamodbEventHandlerComponent;

public class DynamoDbEventRequestHandler implements RequestHandler<DynamodbEvent, StreamsEventResponse> {
    private final BevyTicketDynamodbEventHandlerComponent lambdaComponent = DaggerBevyTicketDynamodbEventHandlerComponent.create();

    @Override
    public StreamsEventResponse handleRequest(final DynamodbEvent input, final Context context) {
        return lambdaComponent.requestHandler().handleRequest(input, context);
    }
}
