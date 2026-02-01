package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.seattleoba.lambda.dagger.DaggerTwitchAccountSyncRequestHandlerComponent;
import org.seattleoba.lambda.dagger.TwitchAccountSyncRequestHandlerComponent;
import org.seattleoba.lambda.model.TwitchAccountSyncResult;

public class AccountSyncRequestHandler implements RequestHandler<Void, TwitchAccountSyncResult> {
    private final TwitchAccountSyncRequestHandlerComponent lambdaComponent = DaggerTwitchAccountSyncRequestHandlerComponent.create();

    @Override
    public TwitchAccountSyncResult handleRequest(final Void unused, final Context context) {
        return lambdaComponent.requestHandler().handleRequest(unused, context);
    }
}
