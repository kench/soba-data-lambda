package org.seattleoba.lambda.model;

/**
 * Result of a Bevy roster import.
 *
 * @param recordsImported number of tickets created or updated
 */
public record BevyRosterImportResult(Integer recordsImported) {}
