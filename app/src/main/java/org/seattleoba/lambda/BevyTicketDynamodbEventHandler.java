package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;

public class BevyTicketDynamodbEventHandler implements RequestHandler<DynamodbEvent, Void> {

    @Override
    public Void handleRequest(final DynamodbEvent input, final Context context) {
        return null;
    }
}
