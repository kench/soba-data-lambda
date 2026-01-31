package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.seattleoba.lambda.dagger.BevyTicketDLQRequestHandlerComponent;
import org.seattleoba.lambda.dagger.DaggerBevyTicketDLQRequestHandlerComponent;
import org.seattleoba.lambda.model.BevyDLQReportResult;

public class DLQRequestHandler implements RequestHandler<Void, BevyDLQReportResult> {
    private final BevyTicketDLQRequestHandlerComponent lambdaComponent = DaggerBevyTicketDLQRequestHandlerComponent.create();

    @Override
    public BevyDLQReportResult handleRequest(final Void input, final Context context) {
        return lambdaComponent.requestHandler().handleRequest(input, context);
    }
}
