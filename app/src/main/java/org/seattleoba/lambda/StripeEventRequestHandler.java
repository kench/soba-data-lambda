package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.seattleoba.lambda.dagger.DaggerStripeEventSqsRequestHandlerComponent;
import org.seattleoba.lambda.dagger.StripeEventSqsRequestHandlerComponent;

/** Lambda entry point that syncs Stripe objects named by events on the StripeEvents queue. */
public class StripeEventRequestHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
  private final StripeEventSqsRequestHandlerComponent lambdaComponent;

  /** Creates the handler with its production dependency graph. */
  public StripeEventRequestHandler() {
    this(DaggerStripeEventSqsRequestHandlerComponent.create());
  }

  /**
   * Creates the handler with the given dependency graph.
   *
   * @param lambdaComponent Dagger component that provides the request handler
   */
  StripeEventRequestHandler(final StripeEventSqsRequestHandlerComponent lambdaComponent) {
    this.lambdaComponent = lambdaComponent;
  }

  @Override
  public SQSBatchResponse handleRequest(final SQSEvent sqsEvent, final Context context) {
    return lambdaComponent.requestHandler().handleRequest(sqsEvent, context);
  }
}
