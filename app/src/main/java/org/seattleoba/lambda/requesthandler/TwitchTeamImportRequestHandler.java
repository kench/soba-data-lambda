package org.seattleoba.lambda.requesthandler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.github.twitch4j.helix.domain.*;
import org.apache.logging.log4j.LogManager;
import org.seattleoba.data.dynamodb.bean.TwitchAccount;
import org.seattleoba.data.dynamodb.bean.TwitchTeam;
import org.seattleoba.data.dynamodb.bean.TwitchTeamMembership;
import org.seattleoba.lambda.model.TwitchTeamImportRequest;
import org.seattleoba.lambda.model.TwitchTeamImportResult;
import org.seattleoba.lambda.twitch.TwitchDataProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;

import javax.inject.Inject;
import java.util.*;

public class TwitchTeamImportRequestHandler implements RequestHandler<TwitchTeamImportRequest, TwitchTeamImportResult> {
    private static final org.apache.logging.log4j.Logger LOG =
            LogManager.getLogger(TwitchTeamImportRequestHandler.class);
    private static final Integer MAX_BATCH_SIZE = 100;

    private final TwitchDataProvider twitchDataProvider;
    private final DynamoDbTable<TwitchAccount> twitchAccountTable;
    private final DynamoDbTable<TwitchTeam> twitchTeamTable;
    private final DynamoDbTable<TwitchTeamMembership> twitchTeamMembershipTable;

    @Inject
    public TwitchTeamImportRequestHandler(
            final TwitchDataProvider twitchDataProvider,
            final DynamoDbEnhancedClient enhancedClient,
            final DynamoDbTable<TwitchAccount> twitchAccountTable,
            final DynamoDbTable<TwitchTeam> twitchTeamTable,
            final DynamoDbTable<TwitchTeamMembership> twitchTeamMembershipTable) {
        this.twitchDataProvider = twitchDataProvider;
        this.twitchAccountTable = twitchAccountTable;
        this.twitchTeamTable = twitchTeamTable;
        this.twitchTeamMembershipTable = twitchTeamMembershipTable;
    }

    @Override
    public TwitchTeamImportResult handleRequest(final TwitchTeamImportRequest request, final Context context) {
        if (Objects.isNull(request.teamId())) {
            LOG.error("Twitch team ID is null for request {}", context.getAwsRequestId());
            throw new IllegalArgumentException("Twitch team ID is null");
        }

        final Optional<Team> team;
        try {
            team = twitchDataProvider.getTeam(request.teamId());
        } catch (final Exception exception) {
            LOG.error("Twitch API call to retrieve team {} failed", request.teamId(), exception);
            throw new RuntimeException(exception);
        }
        if (team.isEmpty()) {
            LOG.error("Twitch team {} not found", request.teamId());
            throw new IllegalArgumentException(String.format("Twitch team %d not found", request.teamId()));
        }

        try {
            persistTeam(team.get());
        } catch (final Exception exception) {
            LOG.error("Unable to persist team information in DynamoDB for team {}", request.teamId());
        }
        team.get().getUsers().forEach(teamMember -> {
            persistTeamMembership(request.teamId(), teamMember);
        });

        persistTeamMemberAccountInformation(team.get().getUsers());
        return new TwitchTeamImportResult(team.get().getUsers().size());
    }

    private void persistTeam(final Team team) {
        final TwitchTeam twitchTeam = new TwitchTeam();
        twitchTeam.setId(Integer.valueOf(team.getId()));
        twitchTeam.setName(team.getTeamName());
        twitchTeam.setDisplayName(team.getTeamDisplayName());
        twitchTeamTable.updateItem(twitchTeam);
    }

    private void persistTeamMembership(final Integer teamId, final TeamUser teamMember) {
        final TwitchTeamMembership membership = new TwitchTeamMembership();
        membership.setTeamId(teamId);
        membership.setUserId(Integer.valueOf(teamMember.getUserId()));
        try {
            twitchTeamMembershipTable.updateItem(membership);
        } catch (final Exception exception) {
            LOG.error(
                    "Unable to persist updated team membership information for user {} and team {}",
                    teamMember.getUserLogin(),
                    teamId,
                    exception);
        }
    }

    private void persistTeamMemberAccountInformation(final Collection<TeamUser> teamMembers) {
        final Iterator<TeamUser> iterator = teamMembers.iterator();

        while (iterator.hasNext()) {
            final List<Integer> userIds = new ArrayList<>();
            while (iterator.hasNext() && userIds.size() < MAX_BATCH_SIZE) {
                final TeamUser teamMember = iterator.next();
                userIds.add(Integer.valueOf(teamMember.getUserId()));
            }
            final Collection<TwitchAccount> twitchAccounts = twitchDataProvider.getTwitchAccountsByUserIds(userIds);
            twitchAccounts.forEach(twitchAccountTable::updateItem);
        }
    }
}
