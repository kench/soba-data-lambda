package org.seattleoba.lambda.dagger;

import dagger.Component;
import javax.inject.Singleton;
import org.seattleoba.data.dagger.DataModule;
import org.seattleoba.lambda.requesthandler.BevyTicketImportRequestHandler;

/** Dagger component for {@link org.seattleoba.lambda.TicketImportRequestHandler}. */
@Singleton
@Component(modules = {AwsModule.class, DataModule.class})
public interface BevyTicketImportRequestHandlerComponent {
  /**
   * Returns the request handler.
   *
   * @return the handler
   */
  BevyTicketImportRequestHandler requestHandler();
}
