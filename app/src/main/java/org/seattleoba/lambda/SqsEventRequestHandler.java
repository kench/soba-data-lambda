package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.seattleoba.lambda.dagger.BevyTicketSqsEventRequestHandlerComponent;
import org.seattleoba.lambda.dagger.DaggerBevyTicketSqsEventRequestHandlerComponent;

/** Lambda entry point that links Bevy tickets from the registration queue to Twitch accounts. */
public class SqsEventRequestHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
  private final BevyTicketSqsEventRequestHandlerComponent lambdaComponent;

  /** Creates the handler with its production dependency graph. */
  public SqsEventRequestHandler() {
    this(DaggerBevyTicketSqsEventRequestHandlerComponent.create());
  }

  /**
   * Creates the handler with the given dependency graph.
   *
   * @param lambdaComponent Dagger component that provides the request handler
   */
  SqsEventRequestHandler(final BevyTicketSqsEventRequestHandlerComponent lambdaComponent) {
    this.lambdaComponent = lambdaComponent;
  }

  @Override
  public SQSBatchResponse handleRequest(final SQSEvent sqsEvent, final Context context) {
    return lambdaComponent.requestHandler().handleRequest(sqsEvent, context);
  }
}
