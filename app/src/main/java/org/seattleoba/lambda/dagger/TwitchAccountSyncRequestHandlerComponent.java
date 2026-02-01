package org.seattleoba.lambda.dagger;

import dagger.Component;
import org.seattleoba.data.dagger.DataModule;
import org.seattleoba.lambda.requesthandler.TwitchAccountSyncRequestHandler;

import javax.inject.Singleton;

@Singleton
@Component(modules = {
        AwsModule.class,
        DataModule.class,
        JacksonModule.class,
        TwitchModule.class})
public interface TwitchAccountSyncRequestHandlerComponent {
    TwitchAccountSyncRequestHandler requestHandler();
}
