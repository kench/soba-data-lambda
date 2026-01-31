package org.seattleoba.lambda.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record BevyTicketErrorEntry(
        @JsonProperty("Event ID") Integer eventId,
        @JsonProperty("Record ID") Integer ticketId,
        @JsonProperty("Ticket ID") String ticketNumber,
        @JsonProperty("Purchaser Name") String purchaserName,
        @JsonProperty("Failure Code") String failureCode) {
}
