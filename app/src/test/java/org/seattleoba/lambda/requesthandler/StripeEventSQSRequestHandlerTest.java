package org.seattleoba.lambda.requesthandler;

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stripe.exception.ApiConnectionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class StripeEventSQSRequestHandlerTest {
    private static final String SNAPSHOT_EVENT = """
            {"version":"0","id":"eb-1","detail-type":"charge.refunded","source":"aws.partner/stripe.com/ed_test",
             "detail":{"id":"evt_1","object":"event","type":"charge.refunded",
                       "data":{"object":{"id":"ch_123","object":"charge","amount":999}}}}
            """;
    private static final String THIN_EVENT = """
            {"version":"0","id":"eb-2","detail-type":"v1.payment_intent.succeeded","source":"aws.partner/stripe.com/ed_test",
             "detail":{"id":"evt_2","object":"v2.core.event","type":"v1.payment_intent.succeeded",
                       "related_object":{"id":"pi_456","type":"payment_intent","url":"/v1/payment_intents/pi_456"}}}
            """;
    private static final String UNSUPPORTED_EVENT = """
            {"detail":{"id":"evt_3","type":"invoice.paid","data":{"object":{"id":"in_789","object":"invoice"}}}}
            """;

    @Mock
    private StripeDataSyncRequestHandler stripeDataSyncRequestHandler;

    private StripeEventSQSRequestHandler requestHandler;

    @BeforeEach
    public void setup() {
        requestHandler = new StripeEventSQSRequestHandler(stripeDataSyncRequestHandler, new ObjectMapper());
    }

    @Test
    public void syncsSnapshotAndThinEvents() throws Exception {
        final SQSBatchResponse response = requestHandler.handleRequest(event(
                message("m1", SNAPSHOT_EVENT), message("m2", THIN_EVENT)), null);

        assertTrue(response.getBatchItemFailures().isEmpty());
        verify(stripeDataSyncRequestHandler).sync("charge", "ch_123");
        verify(stripeDataSyncRequestHandler).sync("payment-intent", "pi_456");
    }

    @Test
    public void skipsUnsupportedObjectTypes() throws Exception {
        final SQSBatchResponse response = requestHandler.handleRequest(event(message("m1", UNSUPPORTED_EVENT)), null);

        assertTrue(response.getBatchItemFailures().isEmpty());
        verify(stripeDataSyncRequestHandler, never()).sync(any(), any());
    }

    @Test
    public void reportsFailedMessages() throws Exception {
        doThrow(new ApiConnectionException("timeout")).when(stripeDataSyncRequestHandler).sync("charge", "ch_123");

        final SQSBatchResponse response = requestHandler.handleRequest(event(
                message("m1", SNAPSHOT_EVENT), message("m2", "not json"), message("m3", THIN_EVENT)), null);

        assertEquals(List.of("m1", "m2"), response.getBatchItemFailures().stream()
                .map(SQSBatchResponse.BatchItemFailure::getItemIdentifier).toList());
        verify(stripeDataSyncRequestHandler).sync("payment-intent", "pi_456");
    }

    private static SQSEvent event(final SQSEvent.SQSMessage... messages) {
        final SQSEvent event = new SQSEvent();
        event.setRecords(List.of(messages));
        return event;
    }

    private static SQSEvent.SQSMessage message(final String id, final String body) {
        final SQSEvent.SQSMessage message = new SQSEvent.SQSMessage();
        message.setMessageId(id);
        message.setBody(body);
        return message;
    }
}
