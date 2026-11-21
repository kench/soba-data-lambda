package org.seattleoba.lambda.model;

/**
 * Result of a Twitch account sync.
 *
 * @param recordsUpdated number of Twitch accounts refreshed
 */
public record TwitchAccountSyncResult(Integer recordsUpdated) {}
