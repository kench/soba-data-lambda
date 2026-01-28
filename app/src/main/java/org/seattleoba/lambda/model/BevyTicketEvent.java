package org.seattleoba.lambda.model;

public record BevyTicketEvent(Integer eventId, Integer ticketId, String ticketNumber, String purchaserName) {
}