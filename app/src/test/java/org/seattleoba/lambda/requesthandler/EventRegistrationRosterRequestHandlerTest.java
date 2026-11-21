package org.seattleoba.lambda.requesthandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.seattleoba.data.dynamodb.bean.BevyTicket;
import org.seattleoba.data.dynamodb.bean.EventRegistration;
import org.seattleoba.data.dynamodb.bean.TwitchAccount;
import org.seattleoba.lambda.model.EventRegistrationRosterRequest;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

class EventRegistrationRosterRequestHandlerTest {
  private final S3Client s3Client = mock(S3Client.class);

  @SuppressWarnings("unchecked")
  private final DynamoDbTable<BevyTicket> ticketTable = mock(DynamoDbTable.class);

  @SuppressWarnings("unchecked")
  private final DynamoDbTable<EventRegistration> registrationTable = mock(DynamoDbTable.class);

  @SuppressWarnings("unchecked")
  private final DynamoDbTable<TwitchAccount> accountTable = mock(DynamoDbTable.class);

  private final EventRegistrationRosterRequestHandler handler =
      new EventRegistrationRosterRequestHandler(
          s3Client, ticketTable, registrationTable, accountTable);

  @Test
  void writesOneRowPerRegisteredTicket() {
    final SdkIterable<Page<BevyTicket>> pages =
        () ->
            List.of(Page.create(List.of(ticket(2, "TTA25060002"), ticket(1, "TTA25060001"))))
                .iterator();
    when(ticketTable.query(any(QueryEnhancedRequest.class))).thenReturn(PageIterable.create(pages));
    final EventRegistration registration = new EventRegistration();
    registration.setTwitchId(114732661);
    when(registrationTable.getItem(any(Key.class))).thenReturn(registration).thenReturn(null);
    final TwitchAccount account = new TwitchAccount();
    account.setUserName("seattleoba");
    when(accountTable.getItem(any(Key.class))).thenReturn(account);
    when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenReturn(PutObjectResponse.builder().build());

    final int processed =
        handler.handleRequest(new EventRegistrationRosterRequest(768), null).recordsProcessed();

    assertEquals(1, processed);
    final ArgumentCaptor<PutObjectRequest> upload = ArgumentCaptor.forClass(PutObjectRequest.class);
    org.mockito.Mockito.verify(s3Client).putObject(upload.capture(), any(RequestBody.class));
    assertEquals("twitch-roster/768.csv", upload.getValue().key());
  }

  @Test
  void failsWhenTheRosterCannotBeUploaded() {
    final SdkIterable<Page<BevyTicket>> pages =
        () -> List.of(Page.<BevyTicket>create(List.of())).iterator();
    when(ticketTable.query(any(QueryEnhancedRequest.class))).thenReturn(PageIterable.create(pages));
    when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenThrow(S3Exception.builder().message("denied").build());

    assertThrows(
        RuntimeException.class,
        () -> handler.handleRequest(new EventRegistrationRosterRequest(768), null));
  }

  private static BevyTicket ticket(final int id, final String ticketId) {
    final BevyTicket ticket = new BevyTicket();
    ticket.setId(id);
    ticket.setTicketId(ticketId);
    ticket.setPurchaseDate(1_762_822_137L);
    return ticket;
  }
}
