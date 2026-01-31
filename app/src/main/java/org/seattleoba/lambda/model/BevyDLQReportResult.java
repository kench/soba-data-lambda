package org.seattleoba.lambda.model;

public record BevyDLQReportResult(Integer recordsReported, String reportBucketName, String reportObjectKey) {
}
