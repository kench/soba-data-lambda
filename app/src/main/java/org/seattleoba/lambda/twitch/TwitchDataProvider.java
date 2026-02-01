package org.seattleoba.lambda.twitch;

import com.github.twitch4j.TwitchClient;
import com.github.twitch4j.helix.domain.Team;
import com.github.twitch4j.helix.domain.TeamList;
import com.github.twitch4j.helix.domain.User;
import com.github.twitch4j.helix.domain.UserList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.seattleoba.data.dynamodb.bean.TwitchAccount;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class TwitchDataProvider {
    private static final Logger LOG = LogManager.getLogger(TwitchDataProvider.class);

    private final TwitchClient twitchClient;

    @Inject
    public TwitchDataProvider(final TwitchClient twitchClient) {
        this.twitchClient = twitchClient;
    }

    public Optional<Team> getTeam(final Integer teamId) {
        final TeamList teamList = twitchClient.getHelix().getTeams(null, String.valueOf(teamId), null).execute();
        if (teamList.getTeams().isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(teamList.getTeams().getFirst());
        }
    }

    public Collection<TwitchAccount> getTwitchAccountsByUserIds(final List<Integer> userIds) {
        final UserList userList = twitchClient.getHelix().getUsers(
                null,
                userIds.stream().map(String::valueOf).toList(),
                null).execute();
        return userList.getUsers().stream()
                .map(this::getTwitchAccount)
                .collect(Collectors.toList());
    }

    public Collection<TwitchAccount> getTwitchAccountsByUserNames(final List<String> userNames) {
        final UserList userList = twitchClient.getHelix().getUsers(
                null,
                null,
                userNames).execute();
        return userList.getUsers().stream()
                .map(this::getTwitchAccount)
                .collect(Collectors.toList());
    }

    private TwitchAccount getTwitchAccount(final User user) {
        final TwitchAccount twitchAccount = new TwitchAccount();
        twitchAccount.setId(Integer.parseInt(user.getId()));
        twitchAccount.setUserName(user.getLogin());
        twitchAccount.setDisplayName(user.getDisplayName());
        twitchAccount.setUserType(user.getType());
        twitchAccount.setBroadcasterType(user.getBroadcasterType());
        twitchAccount.setDescription(user.getDescription());
        twitchAccount.setCreatedAt(user.getCreatedAt().toEpochMilli());
        return twitchAccount;
    }
}
