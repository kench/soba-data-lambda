package org.seattleoba.lambda.dagger;

import dagger.Component;
import org.seattleoba.data.dagger.DataModule;
import org.seattleoba.lambda.requesthandler.BevyTicketImportRequestHandler;

import javax.inject.Singleton;

@Singleton
@Component(modules = {
        AwsModule.class,
        DataModule.class})
public interface BevyTicketImportRequestHandlerComponent {
    BevyTicketImportRequestHandler requestHandler();
}
