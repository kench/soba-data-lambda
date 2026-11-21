package org.seattleoba.lambda.model;

/**
 * Request to import a Twitch team and its members.
 *
 * @param teamId Twitch team ID
 */
public record TwitchTeamImportRequest(Integer teamId) {}
