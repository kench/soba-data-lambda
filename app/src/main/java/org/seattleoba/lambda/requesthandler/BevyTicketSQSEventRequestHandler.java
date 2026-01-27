package org.seattleoba.lambda.requesthandler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.twitch4j.TwitchClient;
import com.github.twitch4j.helix.domain.User;
import com.github.twitch4j.helix.domain.UserList;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.seattleoba.data.dynamodb.bean.EventRegistration;
import org.seattleoba.data.dynamodb.bean.TwitchAccount;
import org.seattleoba.lambda.model.BevyTicketEvent;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteResult;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;

import javax.inject.Inject;
import java.util.*;

public class BevyTicketSQSEventRequestHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
    private static final Logger LOG = LogManager.getLogger(BevyTicketSQSEventRequestHandler.class);

    private final TwitchClient twitchClient;
    private final DynamoDbEnhancedClient enhancedClient;
    private final DynamoDbTable<EventRegistration> eventRegistrationTable;
    private final DynamoDbTable<TwitchAccount> twitchAccountTable;
    private final ObjectMapper objectMapper;

    @Inject
    public BevyTicketSQSEventRequestHandler(
            final TwitchClient twitchClient,
            final DynamoDbEnhancedClient enhancedClient,
            final DynamoDbTable<EventRegistration> eventRegistrationTable,
            final DynamoDbTable<TwitchAccount> twitchAccountTable,
            final ObjectMapper objectMapper) {
        this.twitchClient = twitchClient;
        this.enhancedClient = enhancedClient;
        this.eventRegistrationTable = eventRegistrationTable;
        this.twitchAccountTable = twitchAccountTable;
        this.objectMapper = objectMapper;
    }

    @Override
    public SQSBatchResponse handleRequest(final SQSEvent sqsEvent, final Context context) {
        final List<SQSBatchResponse.BatchItemFailure> batchItemFailures = new ArrayList<>();

        final Map<Integer, String> ticketIdsToMessageIds = new HashMap<>();
        final Map<Integer, Set<Integer>> eventIdToTicketIds = new HashMap<>();
        final BiMap<Integer, String> ticketIdsToUserNames = HashBiMap.create();

        sqsEvent.getRecords().forEach(sqsMessage -> {
            final String messageId = sqsMessage.getMessageId();
            try {
                final BevyTicketEvent bevyTicketEvent =
                        objectMapper.readValue(sqsMessage.getBody(), BevyTicketEvent.class);
                final Integer eventId = bevyTicketEvent.eventId();
                final Integer ticketId = bevyTicketEvent.ticketId();
                final String userName = bevyTicketEvent.purchaserName().toLowerCase(Locale.ROOT);
                if (!eventIdToTicketIds.containsKey(eventId)) {
                    eventIdToTicketIds.put(eventId, new HashSet<>());
                }
                eventIdToTicketIds.get(eventId).add(ticketId);
                ticketIdsToUserNames.put(ticketId, userName);
                ticketIdsToMessageIds.put(ticketId, messageId);
            } catch (final JsonProcessingException exception) {
                LOG.error("Unable to parse message {}", messageId, exception);
                batchItemFailures.add(new SQSBatchResponse.BatchItemFailure(messageId));
            }
        });

        final List<User> users = new ArrayList<>();
        final List<String> userNames = ticketIdsToUserNames.values().stream().toList();
        boolean batchRequestFailed = false;
        try {
            final UserList userList = twitchClient.getHelix().getUsers(
                    null,
                    null,
                    userNames).execute();
            users.addAll(userList.getUsers());
        } catch (final Exception exception) {
            LOG.error("Unable to retrieve Twitch user ID for user names {}", userNames, exception);
            batchRequestFailed = true;
        }

        // Batch request failed, fall back to single requests.
        if (batchRequestFailed) {
            userNames.forEach(userName -> {
                try {
                    final User user = twitchClient.getHelix()
                            .getUsers(null, null, Collections.singletonList(userName))
                            .execute().getUsers().getFirst();
                    users.add(user);
                } catch (final Exception exception) {
                    LOG.error("Unable to retrieve Twitch user ID for user name {}", userName, exception);
                    final Integer ticketId = ticketIdsToUserNames.inverse().get(userName);
                    final String messageId = ticketIdsToMessageIds.get(ticketId);
                    batchItemFailures.add(new SQSBatchResponse.BatchItemFailure(messageId));
                }
            });
        }

        final Map<String, TwitchAccount> twitchAccounts = new HashMap<>();

        users.forEach(user -> {
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
                    batchItemFailures.add(new SQSBatchResponse.BatchItemFailure(ticketIdsToMessageIds.get(ticketId)));
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

        return SQSBatchResponse.builder()
                .withBatchItemFailures(batchItemFailures)
                .build();
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
