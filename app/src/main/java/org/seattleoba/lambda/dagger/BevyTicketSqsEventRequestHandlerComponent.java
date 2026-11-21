package org.seattleoba.lambda.dagger;

import dagger.Component;
import javax.inject.Singleton;
import org.seattleoba.data.dagger.DataModule;
import org.seattleoba.lambda.requesthandler.BevyTicketSqsEventRequestHandler;

/** Dagger component for {@link org.seattleoba.lambda.SqsEventRequestHandler}. */
@Singleton
@Component(modules = {AwsModule.class, DataModule.class, JacksonModule.class, TwitchModule.class})
public interface BevyTicketSqsEventRequestHandlerComponent {
  /**
   * Returns the request handler.
   *
   * @return the handler
   */
  BevyTicketSqsEventRequestHandler requestHandler();
}
