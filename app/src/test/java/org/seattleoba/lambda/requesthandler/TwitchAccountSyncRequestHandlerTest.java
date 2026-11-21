package org.seattleoba.lambda.requesthandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.seattleoba.data.dynamodb.bean.TwitchAccount;
import org.seattleoba.lambda.twitch.TwitchDataProvider;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;

class TwitchAccountSyncRequestHandlerTest {
  @SuppressWarnings("unchecked")
  private final DynamoDbTable<TwitchAccount> table = mock(DynamoDbTable.class);

  private final TwitchDataProvider twitchDataProvider = mock(TwitchDataProvider.class);

  @Test
  void refreshesAccountsInBatchesOfOneHundred() {
    final List<TwitchAccount> stored = new ArrayList<>();
    IntStream.range(0, 150).forEach(i -> stored.add(account(i)));
    final SdkIterable<Page<TwitchAccount>> pages = () -> List.of(Page.create(stored)).iterator();
    when(table.scan()).thenReturn(PageIterable.create(pages));
    when(twitchDataProvider.getTwitchAccountsByUserIds(anyList()))
        .thenReturn(List.of(account(1), account(2)));

    final int updated =
        new TwitchAccountSyncRequestHandler(twitchDataProvider, table)
            .handleRequest(null, null)
            .recordsUpdated();

    verify(twitchDataProvider, times(2)).getTwitchAccountsByUserIds(anyList());
    assertEquals(4, updated);
  }

  private static TwitchAccount account(final int id) {
    final TwitchAccount account = new TwitchAccount();
    account.setId(id);
    return account;
  }
}
