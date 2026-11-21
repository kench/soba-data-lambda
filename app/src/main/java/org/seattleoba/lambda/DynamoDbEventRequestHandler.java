package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;
import org.seattleoba.lambda.dagger.BevyTicketDynamodbEventHandlerComponent;
import org.seattleoba.lambda.dagger.DaggerBevyTicketDynamodbEventHandlerComponent;

/** Lambda entry point for the BevyTickets DynamoDB stream. */
public class DynamoDbEventRequestHandler
    implements RequestHandler<DynamodbEvent, StreamsEventResponse> {
  private final BevyTicketDynamodbEventHandlerComponent lambdaComponent;

  /** Creates the handler with its production dependency graph. */
  public DynamoDbEventRequestHandler() {
    this(DaggerBevyTicketDynamodbEventHandlerComponent.create());
  }

  /**
   * Creates the handler with the given dependency graph.
   *
   * @param lambdaComponent Dagger component that provides the request handler
   */
  DynamoDbEventRequestHandler(final BevyTicketDynamodbEventHandlerComponent lambdaComponent) {
    this.lambdaComponent = lambdaComponent;
  }

  @Override
  public StreamsEventResponse handleRequest(final DynamodbEvent input, final Context context) {
    return lambdaComponent.requestHandler().handleRequest(input, context);
  }
}
