package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.seattleoba.lambda.dagger.DaggerStripeDataSyncRequestHandlerComponent;
import org.seattleoba.lambda.dagger.StripeDataSyncRequestHandlerComponent;
import org.seattleoba.lambda.model.StripeDataSyncRequest;

/** Lambda entry point that syncs one Stripe object on request. */
public class StripeSyncRequestHandler implements RequestHandler<StripeDataSyncRequest, Void> {
  private final StripeDataSyncRequestHandlerComponent stripeDataSyncRequestHandlerComponent;

  /** Creates the handler with its production dependency graph. */
  public StripeSyncRequestHandler() {
    this(DaggerStripeDataSyncRequestHandlerComponent.create());
  }

  /**
   * Creates the handler with the given dependency graph.
   *
   * @param stripeDataSyncRequestHandlerComponent Dagger component that provides the request handler
   */
  StripeSyncRequestHandler(
      final StripeDataSyncRequestHandlerComponent stripeDataSyncRequestHandlerComponent) {
    this.stripeDataSyncRequestHandlerComponent = stripeDataSyncRequestHandlerComponent;
  }

  @Override
  public Void handleRequest(final StripeDataSyncRequest input, final Context context) {
    return stripeDataSyncRequestHandlerComponent.requestHandler().handleRequest(input, context);
  }
}
