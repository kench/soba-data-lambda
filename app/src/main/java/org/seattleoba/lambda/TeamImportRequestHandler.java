package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.seattleoba.lambda.dagger.DaggerTwitchTeamImportRequestHandlerComponent;
import org.seattleoba.lambda.dagger.TwitchTeamImportRequestHandlerComponent;
import org.seattleoba.lambda.model.TwitchTeamImportRequest;
import org.seattleoba.lambda.model.TwitchTeamImportResult;

/** Lambda entry point that imports a Twitch team. */
public class TeamImportRequestHandler
    implements RequestHandler<TwitchTeamImportRequest, TwitchTeamImportResult> {
  private final TwitchTeamImportRequestHandlerComponent lambdaComponent;

  /** Creates the handler with its production dependency graph. */
  public TeamImportRequestHandler() {
    this(DaggerTwitchTeamImportRequestHandlerComponent.create());
  }

  /**
   * Creates the handler with the given dependency graph.
   *
   * @param lambdaComponent Dagger component that provides the request handler
   */
  TeamImportRequestHandler(final TwitchTeamImportRequestHandlerComponent lambdaComponent) {
    this.lambdaComponent = lambdaComponent;
  }

  @Override
  public TwitchTeamImportResult handleRequest(
      final TwitchTeamImportRequest request, final Context context) {
    return lambdaComponent.requestHandler().handleRequest(request, context);
  }
}
