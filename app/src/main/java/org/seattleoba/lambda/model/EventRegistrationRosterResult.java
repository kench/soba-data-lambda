package org.seattleoba.lambda.model;

/**
 * Result of an event registration roster report.
 *
 * @param recordsProcessed number of registrations in the report
 */
public record EventRegistrationRosterResult(Integer recordsProcessed) {}
