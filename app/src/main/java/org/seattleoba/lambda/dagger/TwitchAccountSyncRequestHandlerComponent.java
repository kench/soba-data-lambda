package org.seattleoba.lambda.dagger;

import dagger.Component;
import javax.inject.Singleton;
import org.seattleoba.data.dagger.DataModule;
import org.seattleoba.lambda.requesthandler.TwitchAccountSyncRequestHandler;

/** Dagger component for {@link org.seattleoba.lambda.AccountSyncRequestHandler}. */
@Singleton
@Component(modules = {AwsModule.class, DataModule.class, JacksonModule.class, TwitchModule.class})
public interface TwitchAccountSyncRequestHandlerComponent {
  /**
   * Returns the request handler.
   *
   * @return the handler
   */
  TwitchAccountSyncRequestHandler requestHandler();
}
