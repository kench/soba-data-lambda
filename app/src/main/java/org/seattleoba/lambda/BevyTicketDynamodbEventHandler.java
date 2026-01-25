package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.Record;
import com.github.twitch4j.TwitchClient;
import com.github.twitch4j.helix.domain.UserList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.seattleoba.ticketing.model.TwitchAccount;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.*;

public class BevyTicketDynamodbEventHandler implements RequestHandler<DynamodbEvent, Void> {
    private static final Logger LOG = LogManager.getLogger(BevyTicketDynamodbEventHandler.class);
    private static final String PURCHASER_NAME_FIELD = "purchaser_name";

    private final TwitchClient twitchClient;
    private final String accessToken;

    @Inject
    public BevyTicketDynamodbEventHandler(
            final TwitchClient twitchClient,
            @Named("accessToken") final String accessToken) {
        this.twitchClient = twitchClient;
        this.accessToken = accessToken;
    }

    @Override
    public Void handleRequest(final DynamodbEvent input, final Context context) {
        final Map<Integer, Set<Integer>> eventIdToTicketIds = new HashMap<>();
        final Map<Integer, String> ticketIdsToUserNames = new HashMap<>();

        input.getRecords().stream()
                .filter(record -> !record.getEventName().equals("REMOVE"))
                .map(Record::getDynamodb)
                .forEach(streamRecord -> {
                    LOG.info("Extracted record {}", streamRecord);
                    final Map<String, AttributeValue> newImage = streamRecord.getNewImage();
                    if (Objects.isNull(newImage)) {
                        LOG.error("Record {} is missing newImage field", streamRecord);
                    }
                    if (newImage.containsKey(PURCHASER_NAME_FIELD) &&
                            !newImage.get(PURCHASER_NAME_FIELD).isNULL() &&
                            !newImage.get(PURCHASER_NAME_FIELD).getS().isEmpty()) {
                        final Integer eventId = Integer.parseInt(streamRecord.getNewImage().get("event_id").getN());
                        final Integer ticketId = Integer.parseInt(streamRecord.getNewImage().get("id").getN());
                        final String userName = streamRecord.getNewImage().get(PURCHASER_NAME_FIELD).getS().toLowerCase(Locale.ROOT);
                        if (!eventIdToTicketIds.containsKey(eventId)) {
                            eventIdToTicketIds.put(eventId, new HashSet<>());
                        }
                        eventIdToTicketIds.get(eventId).add(ticketId);
                        ticketIdsToUserNames.put(ticketId, userName);
                    }
                });

        final UserList userList = twitchClient.getHelix().getUsers(
                null,
                null,
                ticketIdsToUserNames.values().stream().toList()).execute();

        final Map<String, TwitchAccount> twitchAccounts = new HashMap<>();

        userList.getUsers().forEach(user -> {
            final Integer userId = Integer.parseInt(user.getId());
            final String broadcasterType = user.getBroadcasterType();
            final String userName = user.getLogin();
            final String displayName = user.getDisplayName();
            final String userType = user.getType();
            final String description = user.getDescription();
            final Long createdAt = user.getCreatedAt().toEpochMilli();
            twitchAccounts.put(userName, new TwitchAccount(
                    userId,
                    userName,
                    displayName,
                    userType,
                    broadcasterType,
                    description,
                    createdAt));
        });

        for (final Map.Entry<Integer, Set<Integer>> eventEntry : eventIdToTicketIds.entrySet()) {
            final Integer eventId = eventEntry.getKey();
            for (final Integer ticketId : eventEntry.getValue()) {
                final String userName = ticketIdsToUserNames.get(ticketId);
                if (twitchAccounts.containsKey(userName)) {
                    final TwitchAccount twitchAccount = twitchAccounts.get(userName);
                    LOG.info(
                            "Adding registration for event {}, ticket {}, and user {}",
                            eventId,
                            ticketId,
                            twitchAccount.displayName());
                }
            }
        }

        return null;
    }
}
