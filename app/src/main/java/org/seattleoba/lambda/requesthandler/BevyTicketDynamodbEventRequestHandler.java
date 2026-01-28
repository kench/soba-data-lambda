package org.seattleoba.lambda.requesthandler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.seattleoba.lambda.model.BevyTicketEvent;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

import javax.inject.Inject;
import java.util.*;

public class BevyTicketDynamodbEventRequestHandler implements RequestHandler<DynamodbEvent, StreamsEventResponse> {
    private static final Logger LOG = LogManager.getLogger(BevyTicketDynamodbEventRequestHandler.class);
    private static final String SQS_QUEUE_URL = System.getenv("SQS_QUEUE_URL");
    private static final Integer MAX_SQS_BATCH_SIZE = 10;
    private static final String EVENT_ID_FIELD_NAME = "event_id";
    private static final String ID_FIELD_NAME = "id";
    private static final String PURCHASER_NAME_FIELD_NAME = "purchaser_name";
    private static final String TICKET_ID_FIELD_NAME = "ticket_id";
    private static final String REMOVE_EVENT_NAME = "REMOVE";

    private final ObjectMapper objectMapper;
    private final SqsClient sqsClient;

    @Inject
    public BevyTicketDynamodbEventRequestHandler(
            final ObjectMapper objectMapper,
            final SqsClient sqsClient) {
        this.objectMapper = objectMapper;
        this.sqsClient = sqsClient;
    }

    @Override
    public StreamsEventResponse handleRequest(final DynamodbEvent input, final Context context) {
        final List<StreamsEventResponse.BatchItemFailure> batchItemFailures = new ArrayList<>();

        final Map<Integer, String> ticketIdToSequenceNumber = new HashMap<>();
        final Set<BevyTicketEvent> bevyTicketEvents = new HashSet<>();
        input.getRecords().forEach(record -> {
            if (!record.getEventName().equals(REMOVE_EVENT_NAME)) {
                final String sequenceNumber = record.getDynamodb().getSequenceNumber();
                final Map<String, AttributeValue> newImage = record.getDynamodb().getNewImage();
                final Integer eventId = Integer.parseInt(newImage.get(EVENT_ID_FIELD_NAME).getN());
                final Integer id = Integer.parseInt(newImage.get(ID_FIELD_NAME).getN());
                final String purchaserName = newImage.get(PURCHASER_NAME_FIELD_NAME).getS();
                final String ticketId = newImage.get(TICKET_ID_FIELD_NAME).getS();
                bevyTicketEvents.add(new BevyTicketEvent(eventId, id, ticketId, purchaserName));
                ticketIdToSequenceNumber.put(id, sequenceNumber);
            }
        });

        final Iterator<BevyTicketEvent> eventIterator = bevyTicketEvents.iterator();
        while (eventIterator.hasNext()) {
            final Collection<String> sequenceNumbers = new HashSet<>();
            final Collection<SendMessageBatchRequestEntry> entries = new ArrayList<>();
            while (eventIterator.hasNext() && entries.size() < MAX_SQS_BATCH_SIZE) {
                final BevyTicketEvent bevyTicketEvent = eventIterator.next();
                final String sequenceNumber = ticketIdToSequenceNumber.get(bevyTicketEvent.ticketId());
                try {
                    entries.add(SendMessageBatchRequestEntry.builder()
                            .id(Integer.toString(bevyTicketEvent.ticketId()))
                            .messageBody(objectMapper.writeValueAsString(bevyTicketEvent))
                            .build());
                    sequenceNumbers.add(sequenceNumber);
                } catch (final Exception exception) {
                    LOG.error("Unable to serialize ticket {} to JSON", bevyTicketEvent, exception);
                    batchItemFailures.add(new StreamsEventResponse.BatchItemFailure(sequenceNumber));
                }
            }

            boolean batchFailure;
            try {
                final SendMessageBatchResponse response = sqsClient.sendMessageBatch(SendMessageBatchRequest.builder()
                        .entries(entries)
                        .queueUrl(SQS_QUEUE_URL)
                        .build());
                batchFailure = response.hasFailed();
            } catch (final Exception exception) {
                LOG.error("Unable to send message batch {} to SQS", entries, exception);
                batchFailure = true;
            }
            if (batchFailure) {
                sequenceNumbers.forEach(sequenceNumber ->
                        batchItemFailures.add(new StreamsEventResponse.BatchItemFailure(sequenceNumber)));
            }
        }

        return StreamsEventResponse.builder()
                .withBatchItemFailures(batchItemFailures)
                .build();
    }

}
