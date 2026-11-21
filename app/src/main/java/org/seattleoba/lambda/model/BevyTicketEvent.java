package org.seattleoba.lambda.model;

/**
 * Bevy ticket change, sent from the BevyTickets stream to the registration queue.
 *
 * @param eventId Bevy event ID
 * @param ticketId numeric Bevy ticket ID
 * @param ticketNumber Bevy ticket number, for example {@code TTA25057051}
 * @param purchaserName name the ticket was bought under, expected to be a Twitch login
 */
public record BevyTicketEvent(
    Integer eventId, Integer ticketId, String ticketNumber, String purchaserName) {}
