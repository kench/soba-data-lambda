package org.seattleoba.lambda.requesthandler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.seattleoba.data.dynamodb.bean.BevyTicket;
import org.seattleoba.lambda.model.BevyDLQReportResult;
import org.seattleoba.lambda.model.BevyTicketErrorEntry;
import org.seattleoba.lambda.model.BevyTicketEvent;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Collectors;

public class BevyTicketDLQEventRequestHandler implements RequestHandler<Void, BevyDLQReportResult> {
    private static final Logger LOG = LogManager.getLogger(BevyTicketDLQEventRequestHandler.class);
    private static final String SQS_DLQ_URL = System.getenv("SQS_QUEUE_URL");
    private static final String REPORT_S3_BUCKET_NAME = System.getenv("S3_BUCKET_NAME");
    private static final Integer MAX_BATCH_SIZE = 10;
    private static final Integer VISIBILITY_TIMEOUT_IN_SECONDS = 60;

    private final SqsClient sqsClient;
    private final S3Client s3Client;
    private final ObjectMapper objectMapper;
    private final DynamoDbTable<BevyTicket> bevyTicketTable;

    @Inject
    public BevyTicketDLQEventRequestHandler(
            final SqsClient sqsClient,
            final S3Client s3Client,
            final ObjectMapper objectMapper,
            final DynamoDbTable<BevyTicket> bevyTicketTable) {
        this.sqsClient = sqsClient;
        this.s3Client = s3Client;
        this.objectMapper = objectMapper;
        this.bevyTicketTable = bevyTicketTable;
    }

    @Override
    public BevyDLQReportResult handleRequest(final Void input, final Context context) {
        final Collection<Message> messages = new HashSet<>();
        ReceiveMessageResponse receiveMessageResponse = receiveMessage();
        while (receiveMessageResponse.hasMessages()) {
            messages.addAll(receiveMessageResponse.messages());
            receiveMessageResponse = receiveMessage();
        }

        final Collection<BevyTicketErrorEntry> errorEntries = new HashSet<>();
        messages.forEach(message -> {
            try {
                final BevyTicketEvent bevyTicketEvent = objectMapper.readValue(message.body(), BevyTicketEvent.class);
                final Integer eventId = bevyTicketEvent.eventId();
                final Integer ticketId = bevyTicketEvent.ticketId();
                final String purchaserName = bevyTicketEvent.purchaserName();
                final String ticketNumber;
                if (Objects.isNull(bevyTicketEvent.ticketNumber()) || bevyTicketEvent.ticketNumber().isEmpty()) {
                    ticketNumber = bevyTicketTable.getItem(Key.builder()
                            .partitionValue(eventId)
                            .sortValue(ticketId)
                            .build()).getTicketId();
                } else {
                    ticketNumber = bevyTicketEvent.ticketNumber();
                }
                final String failureCode;
                if (ticketNumber.isEmpty()) {
                    failureCode = "MISSING_PURCHASER_NAME";
                } else if (!purchaserName.matches("[A-Za-z0-9_]+")) {
                    failureCode = "INVALID_NAME";
                } else {
                    failureCode = "TWITCH_API_ERROR";
                }
                errorEntries.add(new BevyTicketErrorEntry(eventId, ticketId, ticketNumber, purchaserName, failureCode));
            } catch (final Exception exception) {
                LOG.error("Unable to process message {}", message.messageId());
            }
        });

        final CsvMapper mapper = new CsvMapper();
        final CsvSchema csvSchema = mapper.schemaFor(BevyTicketErrorEntry.class).withHeader();
        final File outputFile = new File(String.format("/tmp/output-%d.csv", System.currentTimeMillis()));
        try (final SequenceWriter writer = mapper.writer(csvSchema).writeValues(outputFile)) {
            writer.writeAll(errorEntries);
        } catch (final IOException exception) {
            LOG.error("Unable to write failed entries to file", exception);
            throw new RuntimeException(exception);
        }

        final String objectKey = String.format("error-reports/%d.csv", System.currentTimeMillis());
        final PutObjectResponse putObjectResponse;
        try {
            putObjectResponse = s3Client.putObject(PutObjectRequest.builder()
                    .bucket(REPORT_S3_BUCKET_NAME)
                    .key(objectKey)
                    .build(), RequestBody.fromFile(outputFile));
        } catch (final Exception exception) {
            LOG.error("Unable to upload report {} to S3 bucket {}",
                    objectKey,
                    REPORT_S3_BUCKET_NAME,
                    exception);
            throw new RuntimeException(exception);
        }
        LOG.info("Successfully uploaded report {}", putObjectResponse);

        // Delete processed messages from DLQ
        final Iterator<Message> messageIterator = messages.iterator();
        while (messageIterator.hasNext()) {
            final Collection<String> receiptHandlesBatch = new HashSet<>();
            while (messageIterator.hasNext() && receiptHandlesBatch.size() < MAX_BATCH_SIZE) {
                receiptHandlesBatch.add(messageIterator.next().receiptHandle());
            }
            try {
                final DeleteMessageBatchResponse response = deleteMessages(receiptHandlesBatch);
                response.failed().forEach(failure ->
                        LOG.error("Failed to delete message {}", failure));
            } catch (final Exception exception) {
                LOG.error("Unable to delete messages from SQS queue {}", SQS_DLQ_URL, exception);
            }
        }

        return new BevyDLQReportResult(errorEntries.size(), REPORT_S3_BUCKET_NAME, objectKey);
    }

    private ReceiveMessageResponse receiveMessage() {
        return sqsClient.receiveMessage(ReceiveMessageRequest.builder()
                .maxNumberOfMessages(MAX_BATCH_SIZE)
                .queueUrl(SQS_DLQ_URL)
                .visibilityTimeout(VISIBILITY_TIMEOUT_IN_SECONDS)
                .build());
    }

    private DeleteMessageBatchResponse deleteMessages(final Collection<String> receiptHandles) {
        return sqsClient.deleteMessageBatch(DeleteMessageBatchRequest.builder()
                .entries(receiptHandles.stream()
                        .map(handle -> DeleteMessageBatchRequestEntry.builder()
                                .receiptHandle(handle).build())
                        .collect(Collectors.toSet()))
                .queueUrl(SQS_DLQ_URL)
                .build());
    }
}
