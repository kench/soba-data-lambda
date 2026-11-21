package org.seattleoba.lambda.model;

/**
 * Request to fetch one Stripe object and store it in DynamoDB.
 *
 * @param resourceType hyphenated resource type, for example {@code payment-intent}
 * @param id Stripe object ID
 */
public record StripeDataSyncRequest(String resourceType, String id) {}
