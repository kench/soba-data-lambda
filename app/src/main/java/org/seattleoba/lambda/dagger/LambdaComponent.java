package org.seattleoba.lambda.dagger;

import dagger.Component;
import org.seattleoba.lambda.BevyTicketDynamodbEventHandler;

import javax.inject.Singleton;

@Singleton
@Component(modules = TwitchModule.class)
public interface LambdaComponent {
    BevyTicketDynamodbEventHandler eventHandler();
}
