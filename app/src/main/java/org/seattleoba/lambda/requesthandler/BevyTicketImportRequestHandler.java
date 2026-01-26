package org.seattleoba.lambda.requesthandler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.seattleoba.data.dynamodb.bean.BevyTicket;
import org.seattleoba.data.parser.BevyTicketCsvParser;
import org.seattleoba.data.util.BevyDateUtil;
import org.seattleoba.data.util.BevyTicketNumberUtil;
import org.seattleoba.lambda.model.BevyRosterImportRequest;
import org.seattleoba.lambda.model.BevyRosterImportResult;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import javax.inject.Inject;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class BevyTicketImportRequestHandler implements RequestHandler<BevyRosterImportRequest, BevyRosterImportResult> {
    private final S3Client s3Client;
    private final DynamoDbTable<BevyTicket> bevyTicketDynamoDbTable;

    @Inject
    public BevyTicketImportRequestHandler(
            final S3Client s3Client,
            final DynamoDbTable<BevyTicket> bevyTicketDynamoDbTable) {
        this.s3Client = s3Client;
        this.bevyTicketDynamoDbTable = bevyTicketDynamoDbTable;
    }

    @Override
    public BevyRosterImportResult handleRequest(
            final BevyRosterImportRequest request,
            final Context context) {
        final BevyTicketCsvParser bevyTicketCsvParser = new BevyTicketCsvParser();
        final GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(request.bucketName())
                .key(request.objectKey())
                .build();
        final List<org.seattleoba.data.model.BevyTicket> bevyTickets;

        try (final ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(getObjectRequest)) {
            bevyTickets = bevyTicketCsvParser.parseCsvFile(responseInputStream);
        } catch (final IOException exception) {
            throw new RuntimeException(exception);
        }

        final AtomicInteger recordsImported = new AtomicInteger();
        bevyTickets.stream().map(ticket -> {
            final BevyTicket bevyTicket = new BevyTicket();
            bevyTicket.setAccessCode(ticket.accessCode());
            if (Objects.nonNull(ticket.checkInDate()) && !ticket.checkInDate().isEmpty()) {
                bevyTicket.setCheckInDate(BevyDateUtil.toUnixEpochInSeconds(ticket.checkInDate()));
            }
            bevyTicket.setEventId(request.eventId());
            bevyTicket.setId(BevyTicketNumberUtil.toInteger(ticket.ticketNumber()));
            bevyTicket.setTicketId(ticket.ticketNumber());
            bevyTicket.setOrderId(ticket.orderNumber());
            if (Objects.nonNull(ticket.price()) && !ticket.price().isEmpty()) {
                bevyTicket.setPrice(new BigDecimal(ticket.price()));
            } else {
                bevyTicket.setPrice(new BigDecimal(0));
            }
            if (Objects.nonNull(ticket.purchaseDate()) && !ticket.purchaseDate().isEmpty()) {
                bevyTicket.setPurchaseDate(BevyDateUtil.toUnixEpochInSeconds(ticket.purchaseDate()));
            }
            bevyTicket.setPurchaserName(ticket.purchaserName());
            bevyTicket.setTicketType(ticket.ticketType());
            return bevyTicket;
        }).forEach(ticket -> {
            final BevyTicket existingItem = bevyTicketDynamoDbTable.getItem(ticket);
            if (Objects.isNull(existingItem)) {
                bevyTicketDynamoDbTable.putItem(ticket);
                recordsImported.getAndIncrement();
            } else if (existingItem != ticket) {
                bevyTicketDynamoDbTable.updateItem(ticket);
                recordsImported.getAndIncrement();
            }
        });

        return new BevyRosterImportResult(recordsImported.get());
    }
}
