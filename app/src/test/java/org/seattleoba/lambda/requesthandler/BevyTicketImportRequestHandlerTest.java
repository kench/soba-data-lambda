package org.seattleoba.lambda.requesthandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.seattleoba.data.dynamodb.bean.BevyTicket;
import org.seattleoba.lambda.model.BevyRosterImportRequest;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

class BevyTicketImportRequestHandlerTest {
  private static final String CSV =
      """
      Ticket number,Order number,Paid by (name),Ticket title,Paid date (UTC),Checkin Date (UTC),\
      Access code,Price
      TTA25062005,TTE25065088,SeattleOBA,General Admission,2025-11-11 00:48:57+00:00,\
      2025-11-15 18:30:00+00:00,CODE,12.50
      TTA25062006,TTE25065089,Viewer,Free,,,,
      """;

  private final S3Client s3Client = mock(S3Client.class);

  @SuppressWarnings("unchecked")
  private final DynamoDbTable<BevyTicket> table = mock(DynamoDbTable.class);

  private final BevyTicketImportRequestHandler handler =
      new BevyTicketImportRequestHandler(s3Client, table);

  @Test
  void createsNewTicketsAndUpdatesExistingOnes() {
    when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(csv(CSV));
    when(table.getItem(any(BevyTicket.class))).thenReturn(null).thenReturn(new BevyTicket());

    final int imported =
        handler
            .handleRequest(new BevyRosterImportRequest(768, "bucket", "roster.csv"), null)
            .recordsImported();

    assertEquals(2, imported);
    final ArgumentCaptor<BevyTicket> created = ArgumentCaptor.forClass(BevyTicket.class);
    verify(table).putItem(created.capture());
    assertEquals(768, created.getValue().getEventId());
    assertEquals(62005, created.getValue().getId());
    assertEquals(new BigDecimal("12.50"), created.getValue().getPrice());
    assertEquals(1_762_822_137L, created.getValue().getPurchaseDate());
    final ArgumentCaptor<BevyTicket> updated = ArgumentCaptor.forClass(BevyTicket.class);
    verify(table).updateItem(updated.capture());
    assertEquals(BigDecimal.ZERO, updated.getValue().getPrice());
  }

  @Test
  void failsWhenTheExportCannotBeRead() {
    final InputStream broken =
        new InputStream() {
          @Override
          public int read() throws IOException {
            throw new IOException("reset");
          }

          @Override
          public void close() throws IOException {
            throw new IOException("close failed");
          }
        };
    when(s3Client.getObject(any(GetObjectRequest.class)))
        .thenReturn(new ResponseInputStream<>(GetObjectResponse.builder().build(), broken));

    assertThrows(
        RuntimeException.class,
        () -> handler.handleRequest(new BevyRosterImportRequest(768, "bucket", "key"), null));
  }

  private static ResponseInputStream<GetObjectResponse> csv(final String body) {
    return new ResponseInputStream<>(
        GetObjectResponse.builder().build(),
        new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));
  }
}
