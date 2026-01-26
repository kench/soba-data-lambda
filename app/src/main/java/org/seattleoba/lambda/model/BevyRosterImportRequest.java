package org.seattleoba.lambda.model;

public record BevyRosterImportRequest(
        Integer eventId,
        String bucketName,
        String objectKey) {
}
