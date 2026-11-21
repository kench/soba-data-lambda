package org.seattleoba.lambda.model;

/**
 * Request to import a Bevy roster CSV export from S3.
 *
 * @param eventId Bevy event ID
 * @param bucketName S3 bucket holding the export
 * @param objectKey S3 key of the export
 */
public record BevyRosterImportRequest(Integer eventId, String bucketName, String objectKey) {}
