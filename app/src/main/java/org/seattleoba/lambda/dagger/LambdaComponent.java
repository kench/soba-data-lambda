package org.seattleoba.lambda.dagger;

import dagger.Component;
import org.seattleoba.data.dagger.DataModule;
import org.seattleoba.lambda.BevyTicketDynamodbEventHandler;

import javax.inject.Singleton;

@Singleton
@Component(modules = {
        AwsModule.class,
        DataModule.class,
        TwitchModule.class})
public interface LambdaComponent {
    BevyTicketDynamodbEventHandler eventHandler();
}
