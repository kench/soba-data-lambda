package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.seattleoba.lambda.dagger.DaggerRequestHandlerLambdaComponent;
import org.seattleoba.lambda.dagger.RequestHandlerLambdaComponent;
import org.seattleoba.lambda.model.BevyRosterImportRequest;
import org.seattleoba.lambda.model.BevyRosterImportResult;

public class TicketImportRequestHandler implements RequestHandler<BevyRosterImportRequest, BevyRosterImportResult> {
    private final RequestHandlerLambdaComponent lambdaComponent = DaggerRequestHandlerLambdaComponent.create();

    @Override
    public BevyRosterImportResult handleRequest(final BevyRosterImportRequest request, final Context context) {
        return lambdaComponent.requestHandler().handleRequest(request, context);
    }
}
