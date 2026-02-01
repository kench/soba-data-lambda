package org.seattleoba.lambda.requesthandler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.seattleoba.data.dynamodb.bean.TwitchAccount;
import org.seattleoba.lambda.model.TwitchAccountSyncResult;
import org.seattleoba.lambda.twitch.TwitchDataProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TwitchAccountSyncRequestHandler implements RequestHandler<Void, TwitchAccountSyncResult> {
    private static final Logger LOG = LogManager.getLogger(TwitchAccountSyncRequestHandler.class);
    private static final Integer MAX_BATCH_SIZE = 100;

    private final TwitchDataProvider twitchDataProvider;
    private final DynamoDbTable<TwitchAccount> twitchAccountTable;

    @Inject
    public TwitchAccountSyncRequestHandler(
            final TwitchDataProvider twitchDataProvider,
            final DynamoDbTable<TwitchAccount> twitchAccountTable) {
        this.twitchDataProvider = twitchDataProvider;
        this.twitchAccountTable = twitchAccountTable;
    }

    @Override
    public TwitchAccountSyncResult handleRequest(final Void unused, final Context context) {
        final AtomicInteger recordsUpdated = new AtomicInteger();
        final PageIterable<TwitchAccount> pageIterable = twitchAccountTable.scan();
        pageIterable.stream().forEach(twitchAccountPage -> {
            final Iterator<TwitchAccount> iterator = twitchAccountPage.items().iterator();
            while (iterator.hasNext()) {
                final List<Integer> userIds = new ArrayList<>();
                while (iterator.hasNext() && userIds.size() < MAX_BATCH_SIZE) {
                    userIds.add(iterator.next().getId());
                }
                twitchDataProvider.getTwitchAccountsByUserIds(userIds).forEach(twitchAccount -> {
                    twitchAccountTable.updateItem(twitchAccount);
                    recordsUpdated.incrementAndGet();
                });
            }
            try {
                Thread.sleep(1000);
            } catch (final InterruptedException exception) {
                LOG.error("Sleep interrupted", exception);
                throw new RuntimeException(exception);
            }
        });
        return new TwitchAccountSyncResult(recordsUpdated.get());
    }
}
