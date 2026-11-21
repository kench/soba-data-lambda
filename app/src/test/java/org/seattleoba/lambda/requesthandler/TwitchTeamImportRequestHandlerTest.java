package org.seattleoba.lambda.requesthandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.lambda.runtime.Context;
import com.github.twitch4j.helix.domain.Team;
import com.github.twitch4j.helix.domain.TeamUser;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.seattleoba.data.dynamodb.bean.TwitchAccount;
import org.seattleoba.data.dynamodb.bean.TwitchTeam;
import org.seattleoba.data.dynamodb.bean.TwitchTeamMembership;
import org.seattleoba.lambda.model.TwitchTeamImportRequest;
import org.seattleoba.lambda.twitch.TwitchDataProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;

class TwitchTeamImportRequestHandlerTest {
  private final TwitchDataProvider twitchDataProvider = mock(TwitchDataProvider.class);

  @SuppressWarnings("unchecked")
  private final DynamoDbTable<TwitchAccount> accountTable = mock(DynamoDbTable.class);

  @SuppressWarnings("unchecked")
  private final DynamoDbTable<TwitchTeam> teamTable = mock(DynamoDbTable.class);

  @SuppressWarnings("unchecked")
  private final DynamoDbTable<TwitchTeamMembership> membershipTable = mock(DynamoDbTable.class);

  private final TwitchTeamImportRequestHandler handler =
      new TwitchTeamImportRequestHandler(
          twitchDataProvider,
          mock(DynamoDbEnhancedClient.class),
          accountTable,
          teamTable,
          membershipTable);

  @Test
  void importsTeamMembershipsAndAccounts() {
    final List<TeamUser> users = new ArrayList<>();
    IntStream.range(0, 101).forEach(i -> users.add(user(i)));
    final Team team = team(users);
    when(twitchDataProvider.getTeam(3685)).thenReturn(Optional.of(team));
    when(twitchDataProvider.getTwitchAccountsByUserIds(anyList()))
        .thenReturn(List.of(new TwitchAccount()));
    when(membershipTable.updateItem(any(TwitchTeamMembership.class)))
        .thenThrow(new IllegalStateException("throttled"))
        .thenReturn(null);

    final int imported =
        handler.handleRequest(new TwitchTeamImportRequest(3685), null).recordsImported();

    assertEquals(101, imported);
    verify(teamTable).updateItem(any(TwitchTeam.class));
    verify(membershipTable, times(101)).updateItem(any(TwitchTeamMembership.class));
    verify(twitchDataProvider, times(2)).getTwitchAccountsByUserIds(anyList());
    verify(accountTable, times(2)).updateItem(any(TwitchAccount.class));
  }

  @Test
  void keepsGoingWhenTheTeamCannotBeStored() {
    final Team team = team(List.of());
    when(twitchDataProvider.getTeam(3685)).thenReturn(Optional.of(team));
    when(teamTable.updateItem(any(TwitchTeam.class)))
        .thenThrow(new IllegalStateException("throttled"));

    assertEquals(
        0, handler.handleRequest(new TwitchTeamImportRequest(3685), null).recordsImported());
  }

  @Test
  void rejectsMissingTeamId() {
    final Context context = mock(Context.class);

    assertThrows(
        IllegalArgumentException.class,
        () -> handler.handleRequest(new TwitchTeamImportRequest(null), context));
  }

  @Test
  void rejectsUnknownTeam() {
    when(twitchDataProvider.getTeam(1)).thenReturn(Optional.empty());

    assertThrows(
        IllegalArgumentException.class,
        () -> handler.handleRequest(new TwitchTeamImportRequest(1), null));
  }

  @Test
  void failsWhenTwitchIsUnavailable() {
    when(twitchDataProvider.getTeam(1)).thenThrow(new IllegalStateException("503"));

    assertThrows(
        RuntimeException.class, () -> handler.handleRequest(new TwitchTeamImportRequest(1), null));
  }

  private static Team team(final List<TeamUser> users) {
    final Team team = mock(Team.class);
    when(team.getId()).thenReturn("3685");
    when(team.getTeamName()).thenReturn("soba");
    when(team.getTeamDisplayName()).thenReturn("Seattle Online Broadcasters Association");
    when(team.getUsers()).thenReturn(users);
    return team;
  }

  private static TeamUser user(final int id) {
    final TeamUser user = mock(TeamUser.class);
    when(user.getUserId()).thenReturn(Integer.toString(id));
    org.mockito.Mockito.lenient().when(user.getUserLogin()).thenReturn("user" + id);
    return user;
  }
}
