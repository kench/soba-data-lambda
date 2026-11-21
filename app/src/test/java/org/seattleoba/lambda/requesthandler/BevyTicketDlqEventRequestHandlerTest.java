package org.seattleoba.lambda.requesthandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.seattleoba.data.dynamodb.bean.BevyTicket;
import org.seattleoba.lambda.model.BevyDlqReportResult;
import org.seattleoba.lambda.model.BevyTicketEvent;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

class BevyTicketDlqEventRequestHandlerTest {
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final SqsClient sqsClient = mock(SqsClient.class);
  private final S3Client s3Client = mock(S3Client.class);

  @SuppressWarnings("unchecked")
  private final DynamoDbTable<BevyTicket> table = mock(DynamoDbTable.class);

  private final BevyTicketDlqEventRequestHandler handler =
      new BevyTicketDlqEventRequestHandler(sqsClient, s3Client, objectMapper, table);

  @Test
  void reportsAndDeletesDeadLetters() throws Exception {
    final List<Message> messages = new ArrayList<>();
    messages.add(message("m1", new BevyTicketEvent(768, 1, "TTA25060001", "Not A Login")));
    messages.add(message("m2", new BevyTicketEvent(768, 2, "TTA25060002", "valid_login")));
    messages.add(message("m3", new BevyTicketEvent(768, 3, null, "lookup")));
    messages.add(message("m4", new BevyTicketEvent(768, 4, "", "empty")));
    messages.add(Message.builder().messageId("bad").body("not json").receiptHandle("r").build());
    IntStream.range(10, 20)
        .forEach(i -> messages.add(message("x" + i, new BevyTicketEvent(768, i, "T" + i, "u"))));
    final BevyTicket stored = new BevyTicket();
    stored.setTicketId("TTA25060003");
    when(table.getItem(any(Key.class)))
        .thenAnswer(invocation -> {
          final Key key = invocation.getArgument(0);
          if (key.sortKeyValue().get().n().equals("4")) {
            final BevyTicket blank = new BevyTicket();
            blank.setTicketId("");
            return blank;
          }
          return stored;
        });
    when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
        .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
        .thenReturn(ReceiveMessageResponse.builder().build());
    when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenReturn(PutObjectResponse.builder().build());
    when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
        .thenReturn(
            DeleteMessageBatchResponse.builder()
                .failed(BatchResultErrorEntry.builder().id("1").code("Error").build())
                .build())
        .thenThrow(SqsException.builder().message("throttled").build());

    final BevyDlqReportResult result = handler.handleRequest(null, null);

    assertEquals(14, result.recordsReported());
    assertTrue(result.reportObjectKey().startsWith("error-reports/"));
    verify(sqsClient, times(2)).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
  }

  @Test
  void failsWhenTheReportCannotBeUploaded() {
    when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
        .thenReturn(ReceiveMessageResponse.builder().build());
    when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenThrow(S3Exception.builder().message("denied").build());

    assertThrows(RuntimeException.class, () -> handler.handleRequest(null, null));
  }

  private Message message(final String id, final BevyTicketEvent event) {
    try {
      return Message.builder()
          .messageId(id)
          .receiptHandle("receipt-" + id)
          .body(objectMapper.writeValueAsString(event))
          .build();
    } catch (final Exception exception) {
      throw new IllegalStateException(exception);
    }
  }
}
