package org.seattleoba.lambda.dagger;

import dagger.Component;
import org.seattleoba.lambda.requesthandler.BevyTicketDynamodbEventRequestHandler;

import javax.inject.Singleton;

@Singleton
@Component(modules = {
        AwsModule.class,
        JacksonModule.class})
public interface BevyTicketDynamodbEventHandlerComponent {
    BevyTicketDynamodbEventRequestHandler requestHandler();
}
