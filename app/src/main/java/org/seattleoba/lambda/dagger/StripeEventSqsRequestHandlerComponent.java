package org.seattleoba.lambda.dagger;

import dagger.Component;
import javax.inject.Singleton;
import org.seattleoba.data.dagger.DataModule;
import org.seattleoba.lambda.requesthandler.StripeEventSqsRequestHandler;

/** Dagger component for {@link org.seattleoba.lambda.StripeEventRequestHandler}. */
@Singleton
@Component(modules = {AwsModule.class, DataModule.class, JacksonModule.class, StripeModule.class})
public interface StripeEventSqsRequestHandlerComponent {
  /**
   * Returns the request handler.
   *
   * @return the handler
   */
  StripeEventSqsRequestHandler requestHandler();
}
