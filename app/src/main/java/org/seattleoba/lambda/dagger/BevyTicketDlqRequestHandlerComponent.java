package org.seattleoba.lambda.dagger;

import dagger.Component;
import javax.inject.Singleton;
import org.seattleoba.data.dagger.DataModule;
import org.seattleoba.lambda.requesthandler.BevyTicketDlqEventRequestHandler;

/** Dagger component for {@link org.seattleoba.lambda.DlqRequestHandler}. */
@Singleton
@Component(modules = {AwsModule.class, DataModule.class, JacksonModule.class})
public interface BevyTicketDlqRequestHandlerComponent {
  /**
   * Returns the request handler.
   *
   * @return the handler
   */
  BevyTicketDlqEventRequestHandler requestHandler();
}
