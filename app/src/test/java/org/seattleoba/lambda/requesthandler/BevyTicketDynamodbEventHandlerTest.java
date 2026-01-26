package org.seattleoba.lambda.requesthandler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord;
import com.github.twitch4j.TwitchClient;
import com.github.twitch4j.helix.TwitchHelix;
import com.github.twitch4j.helix.domain.User;
import com.github.twitch4j.helix.domain.UserList;
import com.netflix.hystrix.HystrixCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.seattleoba.data.dynamodb.bean.EventRegistration;
import org.seattleoba.data.dynamodb.bean.TwitchAccount;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.dynamodb.services.local.embedded.DynamoDBEmbedded;

import java.time.Instant;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BevyTicketDynamodbEventHandlerTest {
    private static final Integer EVENT_ID = 488;
    private static final Integer TICKET_ID = 62005;
    private static final Integer TWITCH_ID = 70376773;
    private static final String PURCHASER_NAME = "Kenley";

    @Mock
    private TwitchClient twitchClient;
    @Mock
    private TwitchHelix twitchHelix;
    @Mock
    private HystrixCommand<UserList> userListCommand;
    @Mock
    private UserList userList;
    @Mock
    private User user;
    @Mock
    private DynamodbEvent dynamodbEvent;
    @Mock
    private DynamodbEvent.DynamodbStreamRecord dynamodbStreamRecord;
    @Mock
    private StreamRecord streamRecord;
    @Mock
    private Context context;

    private BevyTicketDynamodbEventHandler eventHandler;

    @BeforeEach
    public void setup() {
        final DynamoDbEnhancedClient dynamoDbEnhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(DynamoDBEmbedded.create().dynamoDbClient())
                .build();
        final DynamoDbTable<TwitchAccount> twitchAccountTable =
                dynamoDbEnhancedClient.table("TwitchAccounts", TableSchema.fromBean(TwitchAccount.class));
        twitchAccountTable.createTable();
        final DynamoDbTable<EventRegistration> eventRegistrationTable =
                dynamoDbEnhancedClient.table("TwitchAccountsBevyTickets", TableSchema.fromBean(EventRegistration.class));
        eventRegistrationTable.createTable();
        eventHandler = new BevyTicketDynamodbEventHandler(
                twitchClient,
                dynamoDbEnhancedClient,
                eventRegistrationTable,
                twitchAccountTable);
    }

    @Test
    void eventHandler() {
        when(dynamodbEvent.getRecords()).thenReturn(Collections.singletonList(dynamodbStreamRecord));
        when(dynamodbStreamRecord.getEventName()).thenReturn("INSERT");
        when(dynamodbStreamRecord.getDynamodb()).thenReturn(streamRecord);
        when(streamRecord.getNewImage()).thenReturn(Map.of(
                "purchaser_name", new AttributeValue(PURCHASER_NAME),
                "event_id", new AttributeValue().withN(String.valueOf(EVENT_ID)),
                "id", new AttributeValue().withN(String.valueOf(TICKET_ID))
        ));
        when(twitchClient.getHelix()).thenReturn(twitchHelix);
        when(twitchHelix.getUsers(any(), any(), any())).thenReturn(userListCommand);
        when(userListCommand.execute()).thenReturn(userList);
        when(userList.getUsers()).thenReturn(Collections.singletonList(user));
        when(user.getId()).thenReturn(TWITCH_ID.toString());
        when(user.getLogin()).thenReturn(PURCHASER_NAME.toLowerCase(Locale.ROOT));
        when(user.getCreatedAt()).thenReturn(Instant.now());

        assertDoesNotThrow(() -> eventHandler.handleRequest(dynamodbEvent, context));
    }
}
