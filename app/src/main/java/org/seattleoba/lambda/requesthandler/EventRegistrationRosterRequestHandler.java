package org.seattleoba.lambda.requesthandler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.seattleoba.data.dynamodb.bean.BevyTicket;
import org.seattleoba.data.dynamodb.bean.EventRegistration;
import org.seattleoba.data.dynamodb.bean.TwitchAccount;
import org.seattleoba.data.util.BevyDateUtil;
import org.seattleoba.lambda.model.EventRegistrationRosterRequest;
import org.seattleoba.lambda.model.EventRegistrationRosterResult;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class EventRegistrationRosterRequestHandler implements
        RequestHandler<EventRegistrationRosterRequest, EventRegistrationRosterResult> {
    private static final Logger LOG = LogManager.getLogger(EventRegistrationRosterRequestHandler.class);
    private static final String REPORT_S3_BUCKET_NAME = System.getenv("S3_BUCKET_NAME");

    private final S3Client s3Client;
    private final DynamoDbTable<BevyTicket> bevyTicketTable;
    private final DynamoDbTable<EventRegistration> eventRegistrationTable;
    private final DynamoDbTable<TwitchAccount> twitchAccountTable;

    @Inject
    public EventRegistrationRosterRequestHandler(
            final S3Client s3Client,
            final DynamoDbTable<BevyTicket> bevyTicketTable,
            final DynamoDbTable<EventRegistration> eventRegistrationTable,
            final DynamoDbTable<TwitchAccount> twitchAccountTable) {
        this.s3Client = s3Client;
        this.bevyTicketTable = bevyTicketTable;
        this.eventRegistrationTable = eventRegistrationTable;
        this.twitchAccountTable = twitchAccountTable;
    }

    @Override
    public EventRegistrationRosterResult handleRequest(
            final EventRegistrationRosterRequest input,
            final Context context) {
        final Integer eventId = input.eventId();
        final QueryConditional queryConditional = QueryConditional.keyEqualTo(
                Key.builder()
                        .partitionValue(eventId)
                        .build());

        final PageIterable<BevyTicket> pageIterable = bevyTicketTable.query(QueryEnhancedRequest.builder()
                .queryConditional(queryConditional)
                .build());

        final List<org.seattleoba.data.model.EventRegistration> eventRegistrations = new ArrayList<>();

        final AtomicInteger recordsProcessed = new AtomicInteger();
        pageIterable.items().stream()
                .sorted(Comparator.comparing(BevyTicket::getTicketId))
                .forEach(bevyTicket -> {
                    final Integer id = bevyTicket.getId();
                    final EventRegistration eventRegistration = eventRegistrationTable.getItem(Key.builder()
                            .partitionValue(id)
                            .build());
                    if (Objects.nonNull(eventRegistration)) {
                        final TwitchAccount twitchAccount = twitchAccountTable.getItem(Key.builder()
                                .partitionValue(eventRegistration.getTwitchId()).build());
                        eventRegistrations.add(new org.seattleoba.data.model.EventRegistration(
                                bevyTicket.getTicketId(),
                                bevyTicket.getOrderId(),
                                bevyTicket.getTicketType(),
                                BevyDateUtil.toBevyDate(bevyTicket.getPurchaseDate()),
                                bevyTicket.getPurchaserName(),
                                twitchAccount.getUserName(),
                                twitchAccount.getBroadcasterType(),
                                twitchAccount.getUserType()));
                        recordsProcessed.set(recordsProcessed.get() + 1);
                    }
        });

        final CsvMapper mapper = new CsvMapper();
        final CsvSchema csvSchema = mapper.schemaFor(org.seattleoba.data.model.EventRegistration.class).withHeader();
        final File outputFile = new File(String.format("/tmp/output-%d.csv", System.currentTimeMillis()));
        try (final SequenceWriter writer = mapper.writer(csvSchema).writeValues(outputFile)) {
            writer.writeAll(eventRegistrations);
        } catch (final IOException exception) {
            LOG.error("Unable to write event registrations to file", exception);
            throw new RuntimeException(exception);
        }

        final String objectKey = String.format("twitch-roster/%d.csv", System.currentTimeMillis());
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

        return new EventRegistrationRosterResult(recordsProcessed.get());
    }
}
