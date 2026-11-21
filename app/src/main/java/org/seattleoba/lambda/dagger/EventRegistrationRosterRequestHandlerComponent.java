package org.seattleoba.lambda.dagger;

import dagger.Component;
import javax.inject.Singleton;
import org.seattleoba.data.dagger.DataModule;
import org.seattleoba.lambda.requesthandler.EventRegistrationRosterRequestHandler;

/** Dagger component for {@link org.seattleoba.lambda.EventRegistrationRosterRequestHandler}. */
@Singleton
@Component(modules = {AwsModule.class, DataModule.class})
public interface EventRegistrationRosterRequestHandlerComponent {
  /**
   * Returns the request handler.
   *
   * @return the handler
   */
  EventRegistrationRosterRequestHandler requestHandler();
}
