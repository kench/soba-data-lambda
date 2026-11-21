package org.seattleoba.lambda.dagger;

import dagger.Component;
import javax.inject.Singleton;
import org.seattleoba.data.dagger.DataModule;
import org.seattleoba.lambda.requesthandler.StripeBackfillRequestHandler;

/** Dagger component for {@link org.seattleoba.lambda.StripeBackfillRequestHandler}. */
@Singleton
@Component(modules = {AwsModule.class, DataModule.class, JacksonModule.class, StripeModule.class})
public interface StripeBackfillRequestHandlerComponent {
  /**
   * Returns the request handler.
   *
   * @return the handler
   */
  StripeBackfillRequestHandler requestHandler();
}
