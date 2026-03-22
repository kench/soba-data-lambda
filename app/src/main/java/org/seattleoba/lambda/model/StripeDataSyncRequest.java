package org.seattleoba.lambda.model;

public record StripeDataSyncRequest(
        String resourceType,
        String id) {
}
