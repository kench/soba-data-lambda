package org.seattleoba.lambda.requesthandler;

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.seattleoba.data.dynamodb.bean.EventRegistration;
import org.seattleoba.data.dynamodb.bean.TwitchAccount;
import org.seattleoba.lambda.model.BevyTicketEvent;
import org.seattleoba.lambda.twitch.TwitchDataProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.dynamodb.services.local.embedded.DynamoDBEmbedded;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BevyTicketSQSEventRequestHandlerTest {
    private static final Integer EVENT_ID = 1;
    private static final String INVALID_USER_NAME = "Invalid User Name";
    private static final String MESSAGE_ID = UUID.randomUUID().toString();
    private static final Integer TICKET_ID = 10;
    private static final Integer USER_ID = 114732661;
    private static final String USER_NAME = "SeattleOBA";

    @Mock
    private TwitchDataProvider twitchDataProvider;
    @Mock
    private SQSEvent sqsEvent;
    @Mock
    private SQSEvent.SQSMessage sqsMessage;
    @Mock
    private TwitchAccount twitchAccount;

    private ObjectMapper objectMapper;
    private BevyTicketSQSEventRequestHandler requestHandler;
    private DynamoDbTable<EventRegistration> eventRegistrationTable;
    private DynamoDbTable<TwitchAccount> twitchAccountTable;

    @BeforeEach
    public void setup() {
        final DynamoDbEnhancedClient dynamoDbEnhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(DynamoDBEmbedded.create().dynamoDbClient())
                .build();
        eventRegistrationTable =
                dynamoDbEnhancedClient.table(
                        "TwitchAccountsBevyTickets",
                        TableSchema.fromBean(EventRegistration.class));
        eventRegistrationTable.createTable();
        twitchAccountTable =
                dynamoDbEnhancedClient.table(
                        "TwitchAccounts",
                        TableSchema.fromBean(TwitchAccount.class));
        twitchAccountTable.createTable();
        objectMapper = new ObjectMapper();
        requestHandler = new BevyTicketSQSEventRequestHandler(
                twitchDataProvider,
                dynamoDbEnhancedClient,
                eventRegistrationTable,
                twitchAccountTable,
                objectMapper);
    }

    @Test
    public void successfullyProcessesEventRegistration() throws JsonProcessingException {
        when(sqsEvent.getRecords()).thenReturn(List.of(sqsMessage));
        when(sqsMessage.getMessageId()).thenReturn(MESSAGE_ID);
        when(sqsMessage.getBody()).thenReturn(objectMapper.writeValueAsString(new BevyTicketEvent(EVENT_ID, TICKET_ID, null, USER_NAME)));
        when(twitchDataProvider.getTwitchAccountsByUserNames(anyList())).thenReturn(Set.of(twitchAccount));
        when(twitchAccount.getUserName()).thenReturn(USER_NAME);
        when(twitchAccount.getId()).thenReturn(USER_ID);

        final SQSBatchResponse sqsBatchResponse = requestHandler.handleRequest(sqsEvent, null);

        assertTrue(sqsBatchResponse.getBatchItemFailures().isEmpty());
        assertNotNull(eventRegistrationTable.getItem(Key.builder().partitionValue(TICKET_ID).build()));
        assertNotNull(twitchAccountTable.getItem(Key.builder().partitionValue(USER_ID).build()));
    }

    @Test
    public void returnsBatchItemFailureWhenTwitchAccountDoesNotExist() throws JsonProcessingException {
        when(sqsEvent.getRecords()).thenReturn(List.of(sqsMessage));
        when(sqsMessage.getMessageId()).thenReturn(MESSAGE_ID);
        when(sqsMessage.getBody()).thenReturn(objectMapper.writeValueAsString(new BevyTicketEvent(EVENT_ID, TICKET_ID, null, USER_NAME)));
        when(twitchDataProvider.getTwitchAccountsByUserNames(anyList())).thenReturn(Set.of());

        final SQSBatchResponse sqsBatchResponse = requestHandler.handleRequest(sqsEvent, null);

        assertFalse(sqsBatchResponse.getBatchItemFailures().isEmpty());
        assertEquals(MESSAGE_ID, sqsBatchResponse.getBatchItemFailures().getFirst().getItemIdentifier());
        assertNull(eventRegistrationTable.getItem(Key.builder().partitionValue(TICKET_ID).build()));
    }

    @Test
    public void returnsBatchItemFailureWhenTwitchCallFails() throws JsonProcessingException {
        when(sqsEvent.getRecords()).thenReturn(List.of(sqsMessage));
        when(sqsMessage.getMessageId()).thenReturn(MESSAGE_ID);
        when(sqsMessage.getBody()).thenReturn(objectMapper.writeValueAsString(new BevyTicketEvent(EVENT_ID, TICKET_ID, null, USER_NAME)));
        when(twitchDataProvider.getTwitchAccountsByUserNames(anyList())).thenThrow(new RuntimeException());

        final SQSBatchResponse sqsBatchResponse = requestHandler.handleRequest(sqsEvent, null);

        assertFalse(sqsBatchResponse.getBatchItemFailures().isEmpty());
        assertEquals(MESSAGE_ID, sqsBatchResponse.getBatchItemFailures().getFirst().getItemIdentifier());
        assertNull(eventRegistrationTable.getItem(Key.builder().partitionValue(TICKET_ID).build()));
    }

    @Test
    public void returnsBatchItemFailureForInvalidPurchaserName() throws JsonProcessingException {
        when(sqsEvent.getRecords()).thenReturn(List.of(sqsMessage));
        when(sqsMessage.getMessageId()).thenReturn(MESSAGE_ID);
        when(sqsMessage.getBody()).thenReturn(objectMapper.writeValueAsString(new BevyTicketEvent(EVENT_ID, TICKET_ID, null, INVALID_USER_NAME)));

        final SQSBatchResponse sqsBatchResponse = requestHandler.handleRequest(sqsEvent, null);

        assertFalse(sqsBatchResponse.getBatchItemFailures().isEmpty());
        assertEquals(MESSAGE_ID, sqsBatchResponse.getBatchItemFailures().getFirst().getItemIdentifier());
        assertNull(eventRegistrationTable.getItem(Key.builder().partitionValue(TICKET_ID).build()));
    }
}
