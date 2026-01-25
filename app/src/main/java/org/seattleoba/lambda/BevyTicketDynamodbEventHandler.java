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
import org.seattleoba.data.dynamodb.bean.EventRegistration;
import org.seattleoba.data.dynamodb.bean.TwitchAccount;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteResult;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;

import javax.inject.Inject;
import java.util.*;

public class BevyTicketDynamodbEventHandler implements RequestHandler<DynamodbEvent, Void> {
    private static final Logger LOG = LogManager.getLogger(BevyTicketDynamodbEventHandler.class);
    private static final String PURCHASER_NAME_FIELD = "purchaser_name";

    private final TwitchClient twitchClient;
    private final DynamoDbEnhancedClient enhancedClient;
    private final DynamoDbTable<EventRegistration> eventRegistrationTable;
    private final DynamoDbTable<TwitchAccount> twitchAccountTable;

    @Inject
    public BevyTicketDynamodbEventHandler(
            final TwitchClient twitchClient,
            final DynamoDbEnhancedClient enhancedClient,
            final DynamoDbTable<EventRegistration> eventRegistrationTable,
            final DynamoDbTable<TwitchAccount> twitchAccountTable) {
        this.twitchClient = twitchClient;
        this.enhancedClient = enhancedClient;
        this.eventRegistrationTable = eventRegistrationTable;
        this.twitchAccountTable = twitchAccountTable;
    }

    @Override
    public Void handleRequest(final DynamodbEvent input, final Context context) {
        final Map<Integer, Set<Integer>> eventIdToTicketIds = new HashMap<>();
        final Map<Integer, String> ticketIdsToUserNames = new HashMap<>();

        input.getRecords().stream()
                .filter(record -> !record.getEventName().equals("REMOVE"))
                .map(Record::getDynamodb)
                .forEach(streamRecord -> {
                    final Map<String, AttributeValue> newImage = streamRecord.getNewImage();
                    if (Objects.isNull(newImage)) {
                        LOG.error("Record {} is missing newImage field", streamRecord);
                    }
                    if (newImage.containsKey(PURCHASER_NAME_FIELD) &&
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
            final TwitchAccount twitchAccount = new TwitchAccount();
            final String userName = user.getLogin();
            twitchAccount.setId(Integer.parseInt(user.getId()));
            twitchAccount.setUserName(userName);
            twitchAccount.setDisplayName(user.getDisplayName());
            twitchAccount.setUserType(user.getType());
            twitchAccount.setBroadcasterType(user.getBroadcasterType());
            twitchAccount.setDescription(user.getDescription());
            twitchAccount.setCreatedAt(user.getCreatedAt().toEpochMilli());
            twitchAccounts.put(userName, twitchAccount);
        });

        final Set<EventRegistration> eventRegistrations = new HashSet<>();

        for (final Map.Entry<Integer, Set<Integer>> eventEntry : eventIdToTicketIds.entrySet()) {
            final Integer eventId = eventEntry.getKey();
            for (final Integer ticketId : eventEntry.getValue()) {
                final String userName = ticketIdsToUserNames.get(ticketId);
                if (twitchAccounts.containsKey(userName)) {
                    final TwitchAccount twitchAccount = twitchAccounts.get(userName);
                    final EventRegistration eventRegistration = new EventRegistration();
                    eventRegistration.setEventId(eventId);
                    eventRegistration.setId(ticketId);
                    eventRegistration.setTwitchId(twitchAccount.getId());
                    eventRegistrations.add(eventRegistration);
                } else {
                    LOG.error("Unable to find Twitch account ({}) for ticket {}", userName, ticketId);
                }
            }
        }

        try {
            persistTwitchAccounts(twitchAccounts.values());
        } catch (final Exception exception) {
            LOG.error("Unable to update Twitch accounts", exception);
            throw new RuntimeException(exception);
        }

        try {
            persistEventRegistrations(eventRegistrations);
        } catch (final Exception exception) {
            LOG.error("Unable to update event registrations", exception);
            throw new RuntimeException(exception);
        }

        return null;
    }

    private void persistTwitchAccounts(final Collection<TwitchAccount> twitchAccounts) {
        if (twitchAccounts.isEmpty()) {
            return;
        }

        WriteBatch.Builder<TwitchAccount> builder = WriteBatch.builder(TwitchAccount.class);
        builder = builder.mappedTableResource(twitchAccountTable);
        for (final TwitchAccount account : twitchAccounts) {
            builder = builder.addPutItem(account);
        }
        final WriteBatch batch = builder.build();
        final BatchWriteResult result = enhancedClient.batchWriteItem(b -> b.writeBatches(batch));
        result.unprocessedPutItemsForTable(twitchAccountTable)
                .forEach(account -> LOG.info(
                        "Unable to write entry for Twitch account {} ({})",
                        account.getDisplayName(),
                        account.getId()));
    }

    private void persistEventRegistrations(final Collection<EventRegistration> eventRegistrations) {
        if (eventRegistrations.isEmpty()) {
            return;
        }

        WriteBatch.Builder<EventRegistration> builder = WriteBatch.builder(EventRegistration.class);
        builder = builder.mappedTableResource(eventRegistrationTable);
        for (final EventRegistration registration : eventRegistrations) {
            builder = builder.addPutItem(registration);
        }
        final WriteBatch batch = builder.build();
        final BatchWriteResult result = enhancedClient.batchWriteItem(b -> b.writeBatches(batch));
        result.unprocessedPutItemsForTable(eventRegistrationTable)
                .forEach(registration -> LOG.error(
                        "Unable to write event registration for ticket ({})",
                        registration.getId()));
    }
}
