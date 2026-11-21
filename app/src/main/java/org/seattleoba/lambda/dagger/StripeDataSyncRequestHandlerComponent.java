package org.seattleoba.lambda.dagger;

import dagger.Component;
import javax.inject.Singleton;
import org.seattleoba.data.dagger.DataModule;
import org.seattleoba.lambda.requesthandler.StripeDataSyncRequestHandler;

/** Dagger component for {@link org.seattleoba.lambda.StripeSyncRequestHandler}. */
@Singleton
@Component(modules = {AwsModule.class, DataModule.class, JacksonModule.class, StripeModule.class})
public interface StripeDataSyncRequestHandlerComponent {
  /**
   * Returns the request handler.
   *
   * @return the handler
   */
  StripeDataSyncRequestHandler requestHandler();
}
