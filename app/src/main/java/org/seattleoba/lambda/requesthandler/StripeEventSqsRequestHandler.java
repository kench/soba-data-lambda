package org.seattleoba.lambda.requesthandler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Consumes Stripe events that EventBridge delivers to the StripeEvents SQS queue and syncs the
 * object each event refers to into DynamoDB.
 *
 * <p>Each SQS message body is an EventBridge envelope whose {@code detail} is the Stripe event.
 * Snapshot events carry the object under {@code detail.data.object}; thin events carry a reference
 * under {@code detail.related_object}. Both are supported. The object is always re-fetched from
 * Stripe so the stored copy is the latest one, whatever order events arrive in.
 */
public class StripeEventSqsRequestHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
  private static final Logger LOG = LogManager.getLogger(StripeEventSqsRequestHandler.class);

  /**
   * Stripe object types mapped to the resource types understood by {@link
   * StripeDataSyncRequestHandler}.
   */
  private static final Map<String, String> OBJECT_TYPE_TO_RESOURCE_TYPE =
      Map.of(
          "balance_transaction", "balance-transaction",
          "charge", "charge",
          "customer", "customer",
          "payment_intent", "payment-intent",
          "refund", "refund");

  private final StripeDataSyncRequestHandler stripeDataSyncRequestHandler;
  private final ObjectMapper objectMapper;

  /**
   * Creates the handler.
   *
   * @param stripeDataSyncRequestHandler handler that fetches and stores Stripe objects
   * @param objectMapper Jackson mapper used to parse EventBridge envelopes
   */
  @Inject
  public StripeEventSqsRequestHandler(
      final StripeDataSyncRequestHandler stripeDataSyncRequestHandler,
      final ObjectMapper objectMapper) {
    this.stripeDataSyncRequestHandler = stripeDataSyncRequestHandler;
    this.objectMapper = objectMapper;
  }

  @Override
  public SQSBatchResponse handleRequest(final SQSEvent sqsEvent, final Context context) {
    final List<SQSBatchResponse.BatchItemFailure> batchItemFailures = new ArrayList<>();
    for (final SQSEvent.SQSMessage message : sqsEvent.getRecords()) {
      final String messageId = message.getMessageId();
      try {
        processMessage(message.getBody());
      } catch (final Exception exception) {
        LOG.error("Error encountered while processing message {}", messageId, exception);
        batchItemFailures.add(new SQSBatchResponse.BatchItemFailure(messageId));
      }
    }
    return SQSBatchResponse.builder().withBatchItemFailures(batchItemFailures).build();
  }

  private void processMessage(final String body) throws Exception {
    final JsonNode detail = objectMapper.readTree(body).path("detail");
    final String eventId = detail.path("id").asText();
    final String eventType = detail.path("type").asText();

    final Optional<StripeObjectReference> reference = getObjectReference(detail);
    if (reference.isEmpty()) {
      LOG.warn("Stripe event {} ({}) does not reference an object; skipping", eventId, eventType);
      return;
    }

    final String resourceType = OBJECT_TYPE_TO_RESOURCE_TYPE.get(reference.get().type());
    if (resourceType == null) {
      LOG.info(
          "Stripe event {} ({}) references unsupported object type {}; skipping",
          eventId,
          eventType,
          reference.get().type());
      return;
    }

    LOG.info(
        "Syncing {} {} for Stripe event {} ({})",
        resourceType,
        reference.get().id(),
        eventId,
        eventType);
    stripeDataSyncRequestHandler.sync(resourceType, reference.get().id());
  }

  static Optional<StripeObjectReference> getObjectReference(final JsonNode detail) {
    final JsonNode dataObject = detail.path("data").path("object");
    if (dataObject.hasNonNull("object") && dataObject.hasNonNull("id")) {
      return Optional.of(
          new StripeObjectReference(
              dataObject.get("object").asText(), dataObject.get("id").asText()));
    }
    final JsonNode relatedObject = detail.path("related_object");
    if (relatedObject.hasNonNull("type") && relatedObject.hasNonNull("id")) {
      return Optional.of(
          new StripeObjectReference(
              relatedObject.get("type").asText(), relatedObject.get("id").asText()));
    }
    return Optional.empty();
  }

  record StripeObjectReference(String type, String id) {}
}
