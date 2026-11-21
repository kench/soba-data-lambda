package org.seattleoba.lambda.model;

/**
 * Result of a Twitch team import.
 *
 * @param recordsImported number of team members imported
 */
public record TwitchTeamImportResult(Integer recordsImported) {}
