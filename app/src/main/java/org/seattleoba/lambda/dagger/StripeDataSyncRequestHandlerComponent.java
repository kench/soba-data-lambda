package org.seattleoba.lambda.dagger;

import dagger.Component;
import org.seattleoba.lambda.requesthandler.StripeDataSyncRequestHandler;

import javax.inject.Singleton;

@Singleton
@Component(modules = {
        AwsModule.class,
        StripeModule.class})
public interface StripeDataSyncRequestHandlerComponent {
    StripeDataSyncRequestHandler requestHandler();
}
