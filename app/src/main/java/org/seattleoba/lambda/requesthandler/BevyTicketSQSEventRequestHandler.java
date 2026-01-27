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

        for (final SQSEvent.SQSMessage message : sqsEvent.getRecords()) {
            final String messageId = message.getMessageId();
            final Integer ticketId;
            final Integer eventId;
            final String purchaserName;
            try {
                final BevyTicketEvent bevyTicketEvent =
                        objectMapper.readValue(message.getBody(), BevyTicketEvent.class);
                ticketId = bevyTicketEvent.ticketId();
                eventId = bevyTicketEvent.eventId();
                purchaserName = bevyTicketEvent.purchaserName();
            } catch (final JsonProcessingException exception) {
                LOG.error("Unable to parse message {}", messageId, exception);
                batchItemFailures.add(new SQSBatchResponse.BatchItemFailure(messageId));
                break;
            }

            final User user;
            final String userName = purchaserName.toLowerCase();
            try {
                final UserList userList = twitchClient.getHelix().getUsers(
                        null,
                        null,
                        Collections.singletonList(userName)).execute();
                user = userList.getUsers().getFirst();
            } catch (final Exception exception) {
                LOG.error("Unable to retrieve Twitch user ID for user name {}", userName, exception);
                batchItemFailures.add(new SQSBatchResponse.BatchItemFailure(messageId));
                break;
            }

            final TwitchAccount twitchAccount = new TwitchAccount();
            twitchAccount.setId(Integer.parseInt(user.getId()));
            twitchAccount.setUserName(user.getLogin());
            twitchAccount.setDisplayName(user.getDisplayName());
            twitchAccount.setUserType(user.getType());
            twitchAccount.setBroadcasterType(user.getBroadcasterType());
            twitchAccount.setDescription(user.getDescription());
            twitchAccount.setCreatedAt(user.getCreatedAt().toEpochMilli());
            final EventRegistration eventRegistration = new EventRegistration();
            eventRegistration.setEventId(eventId);
            eventRegistration.setId(ticketId);
            eventRegistration.setTwitchId(twitchAccount.getId());

            try {
                final TwitchAccount item = twitchAccountTable.getItem(twitchAccount);
                if (Objects.isNull(item)) {
                    twitchAccountTable.putItem(twitchAccount);
                } else if (!twitchAccount.equals(item)) {
                    twitchAccountTable.updateItem(twitchAccount);
                }
            } catch (final Exception exception) {
                LOG.error("Unable to update Twitch accounts", exception);
                batchItemFailures.add(new SQSBatchResponse.BatchItemFailure(messageId));
                break;
            }

            try {
                final EventRegistration item = eventRegistrationTable.getItem(eventRegistration);
                if (Objects.isNull(item)) {
                    eventRegistrationTable.putItem(eventRegistration);
                } else if (!eventRegistration.equals(item)) {
                    eventRegistrationTable.updateItem(eventRegistration);
                }
            } catch (final Exception exception) {
                LOG.error("Unable to update event registrations", exception);
                batchItemFailures.add(new SQSBatchResponse.BatchItemFailure(messageId));
                break;
            }
        }

        return SQSBatchResponse.builder()
                .withBatchItemFailures(batchItemFailures)
                .build();
    }
}
