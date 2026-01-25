package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;

public class BevyTicketDynamodbEventHandler implements RequestHandler<DynamodbEvent, Void> {
    private static final Logger LOG = LogManager.getLogger(BevyTicketDynamodbEventHandler.class);

    private final String clientId;
    private final String clientSecret;

    @Inject
    public BevyTicketDynamodbEventHandler(
            @Named("clientId") final String clientId,
            @Named("clientSecret") final String clientSecret) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    @Override
    public Void handleRequest(final DynamodbEvent input, final Context context) {
        LOG.info(String.format("Twitch client ID is %s", clientId));
        LOG.info(String.format("Twitch client secret is %d characters long", clientSecret.length()));
        return null;
    }
}
