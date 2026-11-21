package org.seattleoba.lambda.requesthandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

class BevyTicketDynamodbEventRequestHandlerTest {
  private final SqsClient sqsClient = mock(SqsClient.class);
  private final BevyTicketDynamodbEventRequestHandler handler =
      new BevyTicketDynamodbEventRequestHandler(new ObjectMapper(), sqsClient);

  @Test
  void sendsInsertedTicketsInBatchesOfTen() {
    when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
        .thenReturn(SendMessageBatchResponse.builder().build());
    final List<DynamodbEvent.DynamodbStreamRecord> records = new ArrayList<>();
    IntStream.range(0, 12).forEach(i -> records.add(record("INSERT", i)));
    records.add(record("REMOVE", 99));

    final StreamsEventResponse response = handler.handleRequest(event(records), null);

    assertTrue(response.getBatchItemFailures().isEmpty());
    verify(sqsClient, times(2)).sendMessageBatch(any(SendMessageBatchRequest.class));
  }

  @Test
  void reportsFailuresWhenSqsRejectsTheBatch() {
    when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
        .thenThrow(SqsException.builder().message("throttled").build());

    final StreamsEventResponse response =
        handler.handleRequest(event(List.of(record("MODIFY", 1))), null);

    assertEquals(1, response.getBatchItemFailures().size());
    assertEquals("seq-1", response.getBatchItemFailures().getFirst().getItemIdentifier());
  }

  @Test
  void reportsFailuresWhenSqsReportsFailedEntries() {
    final SendMessageBatchResponse failed = mock(SendMessageBatchResponse.class);
    when(failed.hasFailed()).thenReturn(true);
    when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(failed);

    final StreamsEventResponse response =
        handler.handleRequest(event(List.of(record("INSERT", 2))), null);

    assertEquals(1, response.getBatchItemFailures().size());
  }

  @Test
  void reportsTicketsThatCannotBeSerialized() throws Exception {
    final ObjectMapper brokenMapper = mock(ObjectMapper.class);
    when(brokenMapper.writeValueAsString(any())).thenThrow(new JsonProcessingException("boom") {});
    when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
        .thenReturn(SendMessageBatchResponse.builder().build());
    final BevyTicketDynamodbEventRequestHandler brokenHandler =
        new BevyTicketDynamodbEventRequestHandler(brokenMapper, sqsClient);

    final StreamsEventResponse response =
        brokenHandler.handleRequest(event(List.of(record("INSERT", 3))), null);

    assertEquals("seq-3", response.getBatchItemFailures().getFirst().getItemIdentifier());
  }

  @Test
  void ignoresRemovals() {
    handler.handleRequest(event(List.of(record("REMOVE", 4))), null);

    verify(sqsClient, never()).sendMessageBatch(any(SendMessageBatchRequest.class));
  }

  private static DynamodbEvent event(final List<DynamodbEvent.DynamodbStreamRecord> records) {
    final DynamodbEvent event = new DynamodbEvent();
    event.setRecords(records);
    return event;
  }

  private static DynamodbEvent.DynamodbStreamRecord record(final String eventName, final int id) {
    final StreamRecord streamRecord = new StreamRecord();
    streamRecord.setSequenceNumber("seq-" + id);
    streamRecord.setNewImage(
        Map.of(
            "event_id", new AttributeValue().withN("768"),
            "id", new AttributeValue().withN(Integer.toString(id)),
            "purchaser_name", new AttributeValue().withS("viewer" + id),
            "ticket_id", new AttributeValue().withS("TTA2506" + id)));
    final DynamodbEvent.DynamodbStreamRecord record = new DynamodbEvent.DynamodbStreamRecord();
    record.setEventName(eventName);
    record.setDynamodb(streamRecord);
    return record;
  }
}
