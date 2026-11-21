package org.seattleoba.lambda.twitch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.twitch4j.TwitchClient;
import com.github.twitch4j.helix.domain.Team;
import com.github.twitch4j.helix.domain.TeamList;
import com.github.twitch4j.helix.domain.User;
import com.github.twitch4j.helix.domain.UserList;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.seattleoba.data.dynamodb.bean.TwitchAccount;

/** Tests for {@link TwitchDataProvider}. */
class TwitchDataProviderTest {
  private final TwitchClient twitchClient = mock(TwitchClient.class, RETURNS_DEEP_STUBS);
  private final TwitchDataProvider twitchDataProvider = new TwitchDataProvider(twitchClient);

  @Test
  void returnsTeamWhenFound() {
    final Team team = mock(Team.class);
    final TeamList teamList = mock(TeamList.class);
    when(teamList.getTeams()).thenReturn(List.of(team));
    when(twitchClient.getHelix().getTeams(null, null, "3685").execute()).thenReturn(teamList);

    assertEquals(Optional.of(team), twitchDataProvider.getTeam(3685));
  }

  @Test
  void returnsEmptyWhenTeamIsMissing() {
    final TeamList teamList = mock(TeamList.class);
    when(teamList.getTeams()).thenReturn(List.of());
    when(twitchClient.getHelix().getTeams(null, null, "1").execute()).thenReturn(teamList);

    assertTrue(twitchDataProvider.getTeam(1).isEmpty());
  }

  @Test
  void mapsUsersLookedUpById() {
    final UserList userList = userList();
    when(twitchClient.getHelix().getUsers(null, List.of("114732661"), null).execute())
        .thenReturn(userList);

    assertAccount(twitchDataProvider.getTwitchAccountsByUserIds(List.of(114732661)));
  }

  @Test
  void mapsUsersLookedUpByName() {
    final UserList userList = userList();
    when(twitchClient.getHelix().getUsers(null, null, List.of("seattleoba")).execute())
        .thenReturn(userList);

    assertAccount(twitchDataProvider.getTwitchAccountsByUserNames(List.of("seattleoba")));
  }

  private static UserList userList() {
    final User user = mock(User.class);
    when(user.getId()).thenReturn("114732661");
    when(user.getLogin()).thenReturn("seattleoba");
    when(user.getDisplayName()).thenReturn("SeattleOBA");
    when(user.getType()).thenReturn("");
    when(user.getBroadcasterType()).thenReturn("affiliate");
    when(user.getDescription()).thenReturn("Seattle Online Broadcasters Association");
    when(user.getCreatedAt()).thenReturn(Instant.ofEpochMilli(1_500_000_000_000L));
    final UserList userList = mock(UserList.class);
    when(userList.getUsers()).thenReturn(List.of(user));
    return userList;
  }

  private static void assertAccount(final Collection<TwitchAccount> accounts) {
    assertEquals(1, accounts.size());
    final TwitchAccount account = accounts.iterator().next();
    assertEquals(114732661, account.getId());
    assertEquals("seattleoba", account.getUserName());
    assertEquals("SeattleOBA", account.getDisplayName());
    assertEquals("", account.getUserType());
    assertEquals("affiliate", account.getBroadcasterType());
    assertEquals("Seattle Online Broadcasters Association", account.getDescription());
    assertEquals(1_500_000_000_000L, account.getCreatedAt());
  }
}
