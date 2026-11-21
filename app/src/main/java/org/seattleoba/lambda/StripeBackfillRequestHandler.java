package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.seattleoba.lambda.dagger.DaggerStripeBackfillRequestHandlerComponent;
import org.seattleoba.lambda.dagger.StripeBackfillRequestHandlerComponent;
import org.seattleoba.lambda.model.StripeBackfillRequest;
import org.seattleoba.lambda.model.StripeBackfillResult;

/** Lambda entry point that backfills Stripe objects into DynamoDB on request. */
public class StripeBackfillRequestHandler
    implements RequestHandler<StripeBackfillRequest, StripeBackfillResult> {
  private final StripeBackfillRequestHandlerComponent stripeBackfillRequestHandlerComponent;

  /** Creates the handler with its production dependency graph. */
  public StripeBackfillRequestHandler() {
    this(DaggerStripeBackfillRequestHandlerComponent.create());
  }

  /**
   * Creates the handler with the given dependency graph.
   *
   * @param stripeBackfillRequestHandlerComponent Dagger component that provides the request handler
   */
  StripeBackfillRequestHandler(
      final StripeBackfillRequestHandlerComponent stripeBackfillRequestHandlerComponent) {
    this.stripeBackfillRequestHandlerComponent = stripeBackfillRequestHandlerComponent;
  }

  @Override
  public StripeBackfillResult handleRequest(
      final StripeBackfillRequest input, final Context context) {
    return stripeBackfillRequestHandlerComponent.requestHandler().handleRequest(input, context);
  }
}
