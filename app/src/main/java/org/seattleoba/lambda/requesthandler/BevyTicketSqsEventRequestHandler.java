package org.seattleoba.lambda.requesthandler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.seattleoba.data.dynamodb.bean.EventRegistration;
import org.seattleoba.data.dynamodb.bean.TwitchAccount;
import org.seattleoba.lambda.model.BevyTicketEvent;
import org.seattleoba.lambda.twitch.TwitchDataProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteResult;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;

/**
 * Links Bevy tickets from the registration queue to the Twitch accounts named by their purchasers,
 * and stores both the registrations and the accounts.
 */
public class BevyTicketSqsEventRequestHandler
    implements RequestHandler<SQSEvent, SQSBatchResponse> {
  private static final Logger LOG = LogManager.getLogger(BevyTicketSqsEventRequestHandler.class);

  /**
   * Maximum batch size. Twitch API supports up to 100 usernames in a single request, but DynamoDB
   * only supports 25 operations in a single batch.
   */
  private static final Integer MAX_BATCH_SIZE = 25;

  private final TwitchDataProvider twitchDataProvider;
  private final DynamoDbEnhancedClient dynamoDbEnhancedClient;
  private final DynamoDbTable<EventRegistration> eventRegistrationTable;
  private final DynamoDbTable<TwitchAccount> twitchAccountTable;
  private final ObjectMapper objectMapper;

  /**
   * Creates the handler.
   *
   * @param twitchDataProvider Twitch API wrapper
   * @param dynamoDbEnhancedClient enhanced DynamoDB client used for batch writes
   * @param eventRegistrationTable TwitchAccountsBevyTickets table
   * @param twitchAccountTable TwitchAccounts table
   * @param objectMapper Jackson mapper used to parse ticket events
   */
  @Inject
  public BevyTicketSqsEventRequestHandler(
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
    sqsEvent
        .getRecords()
        .forEach(message -> {
          final String messageId = message.getMessageId();
          LOG.info("Processing message {}", messageId);
          try {
            final BevyTicketEvent bevyTicketEvent =
                objectMapper.readValue(message.getBody(), BevyTicketEvent.class);
            bevyTicketEvents.add(bevyTicketEvent);
            ticketIdToMessageId.put(bevyTicketEvent.ticketId(), messageId);
            userNameToMessageId.put(
                bevyTicketEvent.purchaserName().toLowerCase(Locale.ROOT), messageId);
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
        twitchDataProvider
            .getTwitchAccountsByUserNames(userNames)
            .forEach(twitchAccount ->
                twitchAccounts.put(
                    twitchAccount.getUserName().toLowerCase(Locale.ROOT), twitchAccount));
      } catch (final Exception exception) {
        LOG.error("Twitch GetUsers API call failed for batch", exception);
        batchFailure = true;
      }

      if (batchFailure) {
        userNames.forEach(userName -> {
          try {
            final TwitchAccount twitchAccount =
                twitchDataProvider
                    .getTwitchAccountsByUserNames(Collections.singletonList(userName))
                    .stream()
                    .findAny()
                    .get();
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
      writeRegistrations(eventRegistrations)
          .forEach(failedEventRegistration ->
              batchItemFailures.add(
                  new SQSBatchResponse.BatchItemFailure(
                      ticketIdToMessageId.get(failedEventRegistration.getId()))));
      writeTwitchAccounts(twitchAccounts.values())
          .forEach(failedTwitchAccount ->
              batchItemFailures.add(
                  new SQSBatchResponse.BatchItemFailure(
                      userNameToMessageId.get(failedTwitchAccount.getUserName()))));
    }

    return SQSBatchResponse.builder().withBatchItemFailures(batchItemFailures).build();
  }

  private Collection<EventRegistration> writeRegistrations(
      final Collection<EventRegistration> eventRegistrations) {
    WriteBatch.Builder<EventRegistration> builder =
        WriteBatch.builder(EventRegistration.class).mappedTableResource(eventRegistrationTable);
    for (final EventRegistration eventRegistration : eventRegistrations) {
      builder = builder.addPutItem(b -> b.item(eventRegistration));
    }
    final WriteBatch writeBatch = builder.build();
    try {
      final BatchWriteResult result =
          dynamoDbEnhancedClient.batchWriteItem(b -> b.writeBatches(writeBatch));
      return result.unprocessedPutItemsForTable(eventRegistrationTable);
    } catch (final Exception exception) {
      LOG.error("Unable to perform batch write of event registrations", exception);
      return eventRegistrations;
    }
  }

  private Collection<TwitchAccount> writeTwitchAccounts(
      final Collection<TwitchAccount> twitchAccounts) {
    WriteBatch.Builder<TwitchAccount> builder =
        WriteBatch.builder(TwitchAccount.class).mappedTableResource(twitchAccountTable);
    for (final TwitchAccount twitchAccount : twitchAccounts) {
      builder = builder.addPutItem(b -> b.item(twitchAccount));
    }
    final WriteBatch writeBatch = builder.build();
    try {
      final BatchWriteResult result =
          dynamoDbEnhancedClient.batchWriteItem(b -> b.writeBatches(writeBatch));
      return result.unprocessedPutItemsForTable(twitchAccountTable);
    } catch (final Exception exception) {
      LOG.error("Unable to perform batch write of Twitch accounts", exception);
      return twitchAccounts;
    }
  }
}
