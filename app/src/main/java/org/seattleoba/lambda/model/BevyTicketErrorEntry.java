package org.seattleoba.lambda.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Row of the Bevy dead-letter queue report.
 *
 * @param eventId Bevy event ID
 * @param ticketId numeric Bevy ticket ID
 * @param ticketNumber Bevy ticket number, for example {@code TTA25057051}
 * @param purchaserName name the ticket was bought under
 * @param failureCode why the ticket couldn't be linked to a Twitch account
 */
public record BevyTicketErrorEntry(
    @JsonProperty("Event ID") Integer eventId,
    @JsonProperty("Record ID") Integer ticketId,
    @JsonProperty("Ticket ID") String ticketNumber,
    @JsonProperty("Purchaser Name") String purchaserName,
    @JsonProperty("Failure Code") String failureCode) {}
