package org.seattleoba.lambda.model;

import java.util.Map;

/**
 * Result of a Stripe backfill run.
 *
 * @param complete true if every requested resource type was fully backfilled
 * @param stored number of objects written, keyed by resource type
 * @param failed number of objects that could not be written to DynamoDB
 * @param nextRequest request that resumes the backfill where this run stopped, or null when the run
 *     is complete
 */
public record StripeBackfillResult(
    boolean complete, Map<String, Integer> stored, int failed, StripeBackfillRequest nextRequest) {}
