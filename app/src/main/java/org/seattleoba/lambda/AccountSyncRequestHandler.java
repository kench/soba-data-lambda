package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.seattleoba.lambda.dagger.DaggerTwitchAccountSyncRequestHandlerComponent;
import org.seattleoba.lambda.dagger.TwitchAccountSyncRequestHandlerComponent;
import org.seattleoba.lambda.model.TwitchAccountSyncResult;

/** Lambda entry point that refreshes stored Twitch accounts. */
public class AccountSyncRequestHandler implements RequestHandler<Void, TwitchAccountSyncResult> {
  private final TwitchAccountSyncRequestHandlerComponent lambdaComponent;

  /** Creates the handler with its production dependency graph. */
  public AccountSyncRequestHandler() {
    this(DaggerTwitchAccountSyncRequestHandlerComponent.create());
  }

  /**
   * Creates the handler with the given dependency graph.
   *
   * @param lambdaComponent Dagger component that provides the request handler
   */
  AccountSyncRequestHandler(final TwitchAccountSyncRequestHandlerComponent lambdaComponent) {
    this.lambdaComponent = lambdaComponent;
  }

  @Override
  public TwitchAccountSyncResult handleRequest(final Void unused, final Context context) {
    return lambdaComponent.requestHandler().handleRequest(unused, context);
  }
}
