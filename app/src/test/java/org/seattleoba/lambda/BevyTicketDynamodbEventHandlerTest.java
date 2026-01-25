package org.seattleoba.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.github.twitch4j.TwitchClient;
import com.github.twitch4j.helix.TwitchHelix;
import com.github.twitch4j.helix.domain.UserList;
import com.netflix.hystrix.HystrixCommand;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BevyTicketDynamodbEventHandlerTest {
    @Mock
    private TwitchClient twitchClient;
    @Mock
    private TwitchHelix twitchHelix;
    @Mock
    private HystrixCommand<UserList> userListCommand;
    @Mock
    private UserList userList;

    @Mock
    private DynamodbEvent dynamodbEvent;
    @Mock
    private Context context;

    @Test
    void eventHandler() {
        when(twitchClient.getHelix()).thenReturn(twitchHelix);
        when(twitchHelix.getUsers(any(), any(), any())).thenReturn(userListCommand);
        when(userListCommand.execute()).thenReturn(userList);
        when(userList.getUsers()).thenReturn(Collections.emptyList());
        BevyTicketDynamodbEventHandler classUnderTest = new BevyTicketDynamodbEventHandler(twitchClient, "");

        assertDoesNotThrow(() -> classUnderTest.handleRequest(dynamodbEvent, context));
    }
}
