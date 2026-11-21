package org.seattleoba.lambda.dagger;

import dagger.Module;
import dagger.Provides;
import javax.inject.Singleton;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

/** Dagger module that provides AWS SDK clients, configured from the Lambda environment. */
@Module
public class AwsModule {
  /**
   * Provides the DynamoDB client.
   *
   * @return the client
   */
  @Provides
  @Singleton
  public DynamoDbClient providesDynamoDbClient() {
    return DynamoDbClient.builder().build();
  }

  /**
   * Provides the enhanced DynamoDB client.
   *
   * @param dynamoDbClient low-level DynamoDB client
   * @return the client
   */
  @Provides
  @Singleton
  public DynamoDbEnhancedClient providesDynamoDbEnhancedClient(
      final DynamoDbClient dynamoDbClient) {
    return DynamoDbEnhancedClient.builder().dynamoDbClient(dynamoDbClient).build();
  }

  /**
   * Provides the S3 client.
   *
   * @return the client
   */
  @Provides
  @Singleton
  public S3Client providesS3Client() {
    return S3Client.builder().build();
  }

  /**
   * Provides the SQS client.
   *
   * @return the client
   */
  @Provides
  @Singleton
  public SqsClient providesSqsClient() {
    return SqsClient.builder().build();
  }
}
