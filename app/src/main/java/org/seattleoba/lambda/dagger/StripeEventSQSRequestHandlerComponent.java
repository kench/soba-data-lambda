package org.seattleoba.lambda.dagger;

import dagger.Component;
import org.seattleoba.data.dagger.DataModule;
import org.seattleoba.lambda.requesthandler.StripeEventSQSRequestHandler;

import javax.inject.Singleton;

@Singleton
@Component(modules = {
        AwsModule.class,
        DataModule.class,
        JacksonModule.class,
        StripeModule.class})
public interface StripeEventSQSRequestHandlerComponent {
    StripeEventSQSRequestHandler requestHandler();
}
