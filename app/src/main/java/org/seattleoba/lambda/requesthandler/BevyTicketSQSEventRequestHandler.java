package org.seattleoba.lambda.requesthandler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.seattleoba.data.dynamodb.bean.EventRegistration;
import org.seattleoba.data.dynamodb.bean.TwitchAccount;
import org.seattleoba.lambda.model.BevyTicketEvent;
import org.seattleoba.lambda.twitch.TwitchDataProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;

import javax.inject.Inject;
import java.util.*;

public class BevyTicketSQSEventRequestHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
    private static final Logger LOG = LogManager.getLogger(BevyTicketSQSEventRequestHandler.class);
    private static final Integer MAX_BATCH_SIZE = 25;

    private final TwitchDataProvider twitchDataProvider;
    private final DynamoDbEnhancedClient dynamoDbEnhancedClient;
    private final DynamoDbTable<EventRegistration> eventRegistrationTable;
    private final DynamoDbTable<TwitchAccount> twitchAccountTable;
    private final ObjectMapper objectMapper;

    @Inject
    public BevyTicketSQSEventRequestHandler(
            final TwitchDataProvider twitchDataProvider,
            final DynamoDbEnhancedClient dynamoDbEnhancedClient,
            final DynamoDbTable<EventRegistration> eventRegistrationTable,
            final DynamoDbTable<TwitchAccount> twitchAccountTable,
            final ObjectMapper objectMapper) {
        this.twitchDataProvider = twitchDataProvider;
        this.dynamoDbEnhancedClient = dynamoDbEnhancedClient;
        this.eventRegistrationTable = eventRegistrationTable;
        this.twitchAccountTable = twitchAccountTable;
        this.objectMapper = objectMapper;
    }

    @Override
    public SQSBatchResponse handleRequest(final SQSEvent sqsEvent, final Context context) {
        final List<SQSBatchResponse.BatchItemFailure> batchItemFailures = new ArrayList<>();

        final Map<Integer, String> ticketIdToMessageId = new HashMap<>();
        final Map<String, String> userNameToMessageId = new HashMap<>();
        final Set<BevyTicketEvent> bevyTicketEvents = new HashSet<>();
        sqsEvent.getRecords().forEach(message -> {
            final String messageId = message.getMessageId();
            LOG.info("Processing message {}", messageId);
            try {
                final BevyTicketEvent bevyTicketEvent =
                        objectMapper.readValue(message.getBody(), BevyTicketEvent.class);
                bevyTicketEvents.add(bevyTicketEvent);
                ticketIdToMessageId.put(bevyTicketEvent.ticketId(), messageId);
                userNameToMessageId.put(bevyTicketEvent.purchaserName().toLowerCase(Locale.ROOT), messageId);
            } catch (final JsonProcessingException exception) {
                LOG.error("Error encountered while processing message {}", messageId, exception);
                batchItemFailures.add(new SQSBatchResponse.BatchItemFailure(messageId));
            }
        });

        final Iterator<BevyTicketEvent> iterator = bevyTicketEvents.iterator();

        while (iterator.hasNext()) {
            final List<String> userNames = new ArrayList<>();
            final Map<String, TwitchAccount> twitchAccounts = new HashMap<>();
            final Map<String, BevyTicketEvent> registrations = new HashMap<>();
            while (iterator.hasNext() && userNames.size() < MAX_BATCH_SIZE) {
                final BevyTicketEvent bevyTicketEvent = iterator.next();
                final String messageId = ticketIdToMessageId.get(bevyTicketEvent.ticketId());
                final String userName = bevyTicketEvent.purchaserName().toLowerCase(Locale.ROOT);

                if (userName.matches("[A-Za-z0-9_]+")) {
                    userNames.add(userName);
                    registrations.put(userName, bevyTicketEvent);
                } else {
                    LOG.error("Purchaser name {} is not a valid Twitch login", userName);
                    batchItemFailures.add(new SQSBatchResponse.BatchItemFailure(messageId));
                }
            }

            boolean batchFailure = false;
            try {
                twitchDataProvider.getTwitchAccountsByUserNames(userNames).forEach(twitchAccount ->
                        twitchAccounts.put(twitchAccount.getUserName().toLowerCase(Locale.ROOT), twitchAccount));
            } catch (final Exception exception) {
                LOG.error("Twitch GetUsers API call failed for batch", exception);
                batchFailure = true;
            }

            if (batchFailure) {
                userNames.forEach(userName -> {
                    try {
                        final TwitchAccount twitchAccount = twitchDataProvider
                                .getTwitchAccountsByUserNames(Collections.singletonList(userName)).stream().findAny().get();
                        twitchAccounts.put(twitchAccount.getUserName().toLowerCase(Locale.ROOT), twitchAccount);
                    } catch (final Exception exception) {
                        LOG.error("Twitch GetUsers API call failed for user {}", userName, exception);
                    }
                });
            }

            final Collection<EventRegistration> eventRegistrations = new HashSet<>();
            for (final BevyTicketEvent registration : registrations.values()) {
                final String userName = registration.purchaserName().toLowerCase(Locale.ROOT);
                final String messageId = ticketIdToMessageId.get(registration.ticketId());
                if (twitchAccounts.containsKey(userName)) {
                    final TwitchAccount twitchAccount = twitchAccounts.get(userName);
                    final EventRegistration eventRegistration = new EventRegistration();
                    eventRegistration.setEventId(registration.eventId());
                    eventRegistration.setId(registration.ticketId());
                    eventRegistration.setTwitchId(twitchAccount.getId());
                    eventRegistrations.add(eventRegistration);
                } else {
                    LOG.error("Unable to find Twitch account for user {}", userName);
                    batchItemFailures.add(new SQSBatchResponse.BatchItemFailure(messageId));
                }
            }
            writeRegistrations(eventRegistrations).forEach(failedEventRegistration ->
                    batchItemFailures.add(
                            new SQSBatchResponse.BatchItemFailure(
                                    ticketIdToMessageId.get(failedEventRegistration.getId()))));
            writeTwitchAccounts(twitchAccounts.values());
        }

        return SQSBatchResponse.builder()
                .withBatchItemFailures(batchItemFailures)
                .build();
    }

    private Collection<EventRegistration> writeRegistrations(final Collection<EventRegistration> eventRegistrations) {
        // TODO: Rewrite to use batchWriteItem API
        final Collection<EventRegistration> failedItems = new HashSet<>();
        for (final EventRegistration eventRegistration : eventRegistrations) {
            try {
                eventRegistrationTable.updateItem(eventRegistration);
            } catch (final Exception exception) {
                LOG.error("Unable to persist event registration {} for user {}",
                        eventRegistration.getId(),
                        eventRegistration.getTwitchId());
                failedItems.add(eventRegistration);
            }
        }
        return failedItems;
    }

    private void writeTwitchAccounts(final Collection<TwitchAccount> twitchAccounts) {
        for (final TwitchAccount twitchAccount : twitchAccounts) {
            try {
                twitchAccountTable.updateItem(twitchAccount);
            } catch (final Exception exception) {
                LOG.error("Unable to persist Twitch account information for user {}", twitchAccount.getDisplayName());
            }
        }
    }
}
