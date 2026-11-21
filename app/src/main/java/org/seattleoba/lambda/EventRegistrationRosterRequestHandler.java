package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.seattleoba.lambda.dagger.DaggerEventRegistrationRosterRequestHandlerComponent;
import org.seattleoba.lambda.dagger.EventRegistrationRosterRequestHandlerComponent;
import org.seattleoba.lambda.model.EventRegistrationRosterRequest;
import org.seattleoba.lambda.model.EventRegistrationRosterResult;

/** Lambda entry point that writes the Twitch roster report for a Bevy event. */
public class EventRegistrationRosterRequestHandler
    implements RequestHandler<EventRegistrationRosterRequest, EventRegistrationRosterResult> {
  private final EventRegistrationRosterRequestHandlerComponent lambdaComponent;

  /** Creates the handler with its production dependency graph. */
  public EventRegistrationRosterRequestHandler() {
    this(DaggerEventRegistrationRosterRequestHandlerComponent.create());
  }

  /**
   * Creates the handler with the given dependency graph.
   *
   * @param lambdaComponent Dagger component that provides the request handler
   */
  EventRegistrationRosterRequestHandler(
      final EventRegistrationRosterRequestHandlerComponent lambdaComponent) {
    this.lambdaComponent = lambdaComponent;
  }

  @Override
  public EventRegistrationRosterResult handleRequest(
      final EventRegistrationRosterRequest input, final Context context) {
    return lambdaComponent.requestHandler().handleRequest(input, context);
  }
}
