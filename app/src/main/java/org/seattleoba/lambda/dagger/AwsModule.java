package org.seattleoba.lambda.dagger;

import dagger.Module;
import dagger.Provides;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import javax.inject.Singleton;

@Module
public class AwsModule {
    @Provides
    @Singleton
    public DynamoDbClient providesDynamoDbClient() {
        return DynamoDbClient.builder().build();
    }

    @Provides
    @Singleton
    public DynamoDbEnhancedClient providesDynamoDbEnhancedClient(final DynamoDbClient dynamoDbClient) {
        return DynamoDbEnhancedClient.builder()
                .dynamoDbClient(dynamoDbClient)
                .build();
    }
}
