package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.seattleoba.lambda.dagger.BevyTicketDlqRequestHandlerComponent;
import org.seattleoba.lambda.dagger.DaggerBevyTicketDlqRequestHandlerComponent;
import org.seattleoba.lambda.model.BevyDlqReportResult;

/** Lambda entry point that reports and clears the registration dead-letter queue. */
public class DlqRequestHandler implements RequestHandler<Void, BevyDlqReportResult> {
  private final BevyTicketDlqRequestHandlerComponent lambdaComponent;

  /** Creates the handler with its production dependency graph. */
  public DlqRequestHandler() {
    this(DaggerBevyTicketDlqRequestHandlerComponent.create());
  }

  /**
   * Creates the handler with the given dependency graph.
   *
   * @param lambdaComponent Dagger component that provides the request handler
   */
  DlqRequestHandler(final BevyTicketDlqRequestHandlerComponent lambdaComponent) {
    this.lambdaComponent = lambdaComponent;
  }

  @Override
  public BevyDlqReportResult handleRequest(final Void input, final Context context) {
    return lambdaComponent.requestHandler().handleRequest(input, context);
  }
}
