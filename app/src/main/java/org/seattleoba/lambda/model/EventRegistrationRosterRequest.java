package org.seattleoba.lambda.model;

/**
 * Request to build the Twitch roster report for a Bevy event.
 *
 * @param eventId Bevy event ID
 */
public record EventRegistrationRosterRequest(Integer eventId) {}
