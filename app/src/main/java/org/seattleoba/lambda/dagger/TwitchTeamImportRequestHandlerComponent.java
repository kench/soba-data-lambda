package org.seattleoba.lambda.dagger;

import dagger.Component;
import javax.inject.Singleton;
import org.seattleoba.data.dagger.DataModule;
import org.seattleoba.lambda.requesthandler.TwitchTeamImportRequestHandler;

/** Dagger component for {@link org.seattleoba.lambda.TeamImportRequestHandler}. */
@Singleton
@Component(modules = {AwsModule.class, DataModule.class, JacksonModule.class, TwitchModule.class})
public interface TwitchTeamImportRequestHandlerComponent {
  /**
   * Returns the request handler.
   *
   * @return the handler
   */
  TwitchTeamImportRequestHandler requestHandler();
}
