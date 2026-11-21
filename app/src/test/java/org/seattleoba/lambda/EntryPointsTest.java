package org.seattleoba.lambda;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;
import org.junit.jupiter.api.Test;
import org.seattleoba.lambda.dagger.BevyTicketDlqRequestHandlerComponent;
import org.seattleoba.lambda.dagger.BevyTicketDynamodbEventHandlerComponent;
import org.seattleoba.lambda.dagger.BevyTicketImportRequestHandlerComponent;
import org.seattleoba.lambda.dagger.BevyTicketSqsEventRequestHandlerComponent;
import org.seattleoba.lambda.dagger.EventRegistrationRosterRequestHandlerComponent;
import org.seattleoba.lambda.dagger.StripeDataSyncRequestHandlerComponent;
import org.seattleoba.lambda.dagger.StripeEventSqsRequestHandlerComponent;
import org.seattleoba.lambda.dagger.TwitchAccountSyncRequestHandlerComponent;
import org.seattleoba.lambda.dagger.TwitchTeamImportRequestHandlerComponent;
import org.seattleoba.lambda.model.BevyDlqReportResult;
import org.seattleoba.lambda.model.BevyRosterImportRequest;
import org.seattleoba.lambda.model.BevyRosterImportResult;
import org.seattleoba.lambda.model.EventRegistrationRosterRequest;
import org.seattleoba.lambda.model.EventRegistrationRosterResult;
import org.seattleoba.lambda.model.StripeDataSyncRequest;
import org.seattleoba.lambda.model.TwitchAccountSyncResult;
import org.seattleoba.lambda.model.TwitchTeamImportRequest;
import org.seattleoba.lambda.model.TwitchTeamImportResult;

/** Checks that each Lambda entry point builds its graph and delegates to its handler. */
class EntryPointsTest {
  private final Context context = mock(Context.class);

  @Test
  void productionGraphsBuildWithoutCallingAws() {
    assertNotNull(new AccountSyncRequestHandler());
    assertNotNull(new DlqRequestHandler());
    assertNotNull(new DynamoDbEventRequestHandler());
    assertNotNull(new EventRegistrationRosterRequestHandler());
    assertNotNull(new SqsEventRequestHandler());
    assertNotNull(new StripeEventRequestHandler());
    assertNotNull(new StripeSyncRequestHandler());
    assertNotNull(new TeamImportRequestHandler());
    assertNotNull(new TicketImportRequestHandler());
  }

  @Test
  void accountSyncDelegates() {
    final var component = mock(TwitchAccountSyncRequestHandlerComponent.class, RETURNS_DEEP_STUBS);
    final TwitchAccountSyncResult result = new TwitchAccountSyncResult(3);
    when(component.requestHandler().handleRequest(null, context)).thenReturn(result);

    assertSame(result, new AccountSyncRequestHandler(component).handleRequest(null, context));
  }

  @Test
  void dlqDelegates() {
    final var component = mock(BevyTicketDlqRequestHandlerComponent.class, RETURNS_DEEP_STUBS);
    final BevyDlqReportResult result = new BevyDlqReportResult(1, "bucket", "key");
    when(component.requestHandler().handleRequest(null, context)).thenReturn(result);

    assertSame(result, new DlqRequestHandler(component).handleRequest(null, context));
  }

  @Test
  void dynamoDbEventDelegates() {
    final var component = mock(BevyTicketDynamodbEventHandlerComponent.class, RETURNS_DEEP_STUBS);
    final DynamodbEvent event = new DynamodbEvent();
    final StreamsEventResponse result = new StreamsEventResponse();
    when(component.requestHandler().handleRequest(event, context)).thenReturn(result);

    assertSame(result, new DynamoDbEventRequestHandler(component).handleRequest(event, context));
  }

  @Test
  void eventRegistrationRosterDelegates() {
    final var component =
        mock(EventRegistrationRosterRequestHandlerComponent.class, RETURNS_DEEP_STUBS);
    final EventRegistrationRosterRequest request = new EventRegistrationRosterRequest(7);
    final EventRegistrationRosterResult result = new EventRegistrationRosterResult(2);
    when(component.requestHandler().handleRequest(request, context)).thenReturn(result);

    assertSame(
        result,
        new EventRegistrationRosterRequestHandler(component).handleRequest(request, context));
  }

  @Test
  void sqsEventDelegates() {
    final var component = mock(BevyTicketSqsEventRequestHandlerComponent.class, RETURNS_DEEP_STUBS);
    final SQSEvent event = new SQSEvent();
    final SQSBatchResponse result = new SQSBatchResponse();
    when(component.requestHandler().handleRequest(event, context)).thenReturn(result);

    assertSame(result, new SqsEventRequestHandler(component).handleRequest(event, context));
  }

  @Test
  void stripeEventDelegates() {
    final var component = mock(StripeEventSqsRequestHandlerComponent.class, RETURNS_DEEP_STUBS);
    final SQSEvent event = new SQSEvent();
    final SQSBatchResponse result = new SQSBatchResponse();
    when(component.requestHandler().handleRequest(event, context)).thenReturn(result);

    assertSame(result, new StripeEventRequestHandler(component).handleRequest(event, context));
  }

  @Test
  void stripeSyncDelegates() {
    final var component = mock(StripeDataSyncRequestHandlerComponent.class, RETURNS_DEEP_STUBS);
    final StripeDataSyncRequest request = new StripeDataSyncRequest("charge", "ch_1");

    new StripeSyncRequestHandler(component).handleRequest(request, context);

    org.mockito.Mockito.verify(component.requestHandler()).handleRequest(request, context);
  }

  @Test
  void teamImportDelegates() {
    final var component = mock(TwitchTeamImportRequestHandlerComponent.class, RETURNS_DEEP_STUBS);
    final TwitchTeamImportRequest request = new TwitchTeamImportRequest(5);
    final TwitchTeamImportResult result = new TwitchTeamImportResult(4);
    when(component.requestHandler().handleRequest(request, context)).thenReturn(result);

    assertSame(result, new TeamImportRequestHandler(component).handleRequest(request, context));
  }

  @Test
  void ticketImportDelegates() {
    final var component = mock(BevyTicketImportRequestHandlerComponent.class, RETURNS_DEEP_STUBS);
    final BevyRosterImportRequest request = new BevyRosterImportRequest(1, "bucket", "key");
    final BevyRosterImportResult result = new BevyRosterImportResult(9);
    when(component.requestHandler().handleRequest(request, context)).thenReturn(result);

    assertSame(result, new TicketImportRequestHandler(component).handleRequest(request, context));
  }
}
