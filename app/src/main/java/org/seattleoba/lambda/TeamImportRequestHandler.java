package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.seattleoba.lambda.dagger.DaggerTwitchTeamImportRequestHandlerComponent;
import org.seattleoba.lambda.dagger.TwitchTeamImportRequestHandlerComponent;
import org.seattleoba.lambda.model.TwitchTeamImportRequest;
import org.seattleoba.lambda.model.TwitchTeamImportResult;

public class TeamImportRequestHandler implements RequestHandler<TwitchTeamImportRequest, TwitchTeamImportResult> {
    private final TwitchTeamImportRequestHandlerComponent lambdaComponent = DaggerTwitchTeamImportRequestHandlerComponent.create();

    @Override
    public TwitchTeamImportResult handleRequest(
            final TwitchTeamImportRequest request,
            final Context context) {
        return lambdaComponent.requestHandler().handleRequest(request, context);
    }
}
