package org.seattleoba.lambda.model;

/**
 * Result of a Bevy dead-letter queue report.
 *
 * @param recordsReported number of failed tickets in the report
 * @param reportBucketName S3 bucket holding the report
 * @param reportObjectKey S3 key of the CSV report
 */
public record BevyDlqReportResult(
    Integer recordsReported, String reportBucketName, String reportObjectKey) {}
