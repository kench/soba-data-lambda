package org.seattleoba.lambda.model;

import java.util.List;

/**
 * Request to copy Stripe objects into DynamoDB by paging through the Stripe list APIs.
 *
 * <p>Every field is optional. An empty request backfills every supported resource type for all
 * time. A {@link StripeBackfillResult#nextRequest()} from an earlier run can be sent as-is to pick
 * up where that run stopped.
 *
 * @param resourceTypes hyphenated resource types to backfill, in order, for example {@code charge}
 *     or {@code payment-intent}; null or empty means all of them
 * @param createdGte only objects created at or after this Unix time, in seconds
 * @param createdLte only objects created at or before this Unix time, in seconds
 * @param startingAfter Stripe object ID to resume after, applied to the first resource type only
 */
public record StripeBackfillRequest(
    List<String> resourceTypes, Long createdGte, Long createdLte, String startingAfter) {}
