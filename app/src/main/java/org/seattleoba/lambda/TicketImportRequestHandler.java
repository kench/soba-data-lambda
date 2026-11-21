package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.seattleoba.lambda.dagger.BevyTicketImportRequestHandlerComponent;
import org.seattleoba.lambda.dagger.DaggerBevyTicketImportRequestHandlerComponent;
import org.seattleoba.lambda.model.BevyRosterImportRequest;
import org.seattleoba.lambda.model.BevyRosterImportResult;

/** Lambda entry point that imports a Bevy roster export. */
public class TicketImportRequestHandler
    implements RequestHandler<BevyRosterImportRequest, BevyRosterImportResult> {
  private final BevyTicketImportRequestHandlerComponent lambdaComponent;

  /** Creates the handler with its production dependency graph. */
  public TicketImportRequestHandler() {
    this(DaggerBevyTicketImportRequestHandlerComponent.create());
  }

  /**
   * Creates the handler with the given dependency graph.
   *
   * @param lambdaComponent Dagger component that provides the request handler
   */
  TicketImportRequestHandler(final BevyTicketImportRequestHandlerComponent lambdaComponent) {
    this.lambdaComponent = lambdaComponent;
  }

  @Override
  public BevyRosterImportResult handleRequest(
      final BevyRosterImportRequest request, final Context context) {
    return lambdaComponent.requestHandler().handleRequest(request, context);
  }
}
