package org.seattleoba.lambda.dagger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.philippheuer.credentialmanager.domain.OAuth2Credential;
import com.github.twitch4j.TwitchClient;
import com.github.twitch4j.auth.providers.TwitchIdentityProvider;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** Tests for the Dagger modules. */
class ModulesTest {
  private static final String SECRETS_EXTENSION_RESPONSE =
      "{\"ARN\":\"arn\",\"Name\":\"name\",\"SecretString\":\"s3cr3t\",\"VersionId\":\"v1\"}";

  @BeforeAll
  static void setRegion() {
    System.setProperty("aws.region", "us-west-2");
  }

  @AfterAll
  static void clearRegion() {
    System.clearProperty("aws.region");
  }

  @Test
  void awsModuleBuildsClients() {
    final AwsModule awsModule = new AwsModule();
    final DynamoDbClient dynamoDbClient = awsModule.providesDynamoDbClient();

    assertNotNull(awsModule.providesDynamoDbEnhancedClient(dynamoDbClient));
    assertNotNull(awsModule.providesS3Client());
    assertNotNull(awsModule.providesSqsClient());
  }

  @Test
  void jacksonModuleIgnoresUnknownProperties() {
    final ObjectMapper objectMapper = new JacksonModule().providesObjectMapper();

    assertFalse(objectMapper.isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES));
  }

  @Test
  void stripeModuleReadsSecretFromExtension() throws Exception {
    final StripeModule stripeModule = new StripeModule();

    assertNotNull(stripeModule.providesHttpClient());
    assertEquals("s3cr3t", stripeModule.providesStripeSecretKey(secretsClient()));
    assertNotNull(stripeModule.providesStripeClient("sk_test_123"));
  }

  @Test
  void stripeModuleFailsWhenExtensionIsUnreachable() throws Exception {
    final HttpClient client = mock(HttpClient.class);
    when(client.send(any(), any())).thenThrow(new IOException("connection refused"));

    assertThrows(
        IllegalStateException.class, () -> new StripeModule().providesStripeSecretKey(client));
  }

  @Test
  void twitchModuleReadsSecretFromExtension() throws Exception {
    final TwitchModule twitchModule = new TwitchModule();

    assertNotNull(twitchModule.providesHttpClient());
    assertEquals(System.getenv("CLIENT_ID"), twitchModule.providesClientId());
    assertEquals("s3cr3t", twitchModule.providesClientSecret(secretsClient()));
  }

  @Test
  void twitchModuleFailsWhenExtensionIsUnreachable() throws Exception {
    final HttpClient client = mock(HttpClient.class);
    when(client.send(any(), any())).thenThrow(new IOException("connection refused"));

    assertThrows(
        IllegalStateException.class, () -> new TwitchModule().providesClientSecret(client));
  }

  @Test
  void twitchModuleBuildsProviders() {
    final TwitchModule twitchModule = new TwitchModule();
    final TwitchIdentityProvider identityProvider =
        twitchModule.providesTwitchIdentityProvider("client-id", "client-secret");

    assertNotNull(identityProvider);
    assertNotNull(twitchModule.providesTwitchDataProvider(mock(TwitchClient.class)));
  }

  @Test
  void twitchModuleReturnsAppAccessToken() {
    final TwitchIdentityProvider identityProvider =
        mock(TwitchIdentityProvider.class, RETURNS_DEEP_STUBS);
    final OAuth2Credential credential = mock(OAuth2Credential.class);
    when(credential.getAccessToken()).thenReturn("token");
    when(identityProvider.getAppAccessToken()).thenReturn(credential);

    assertEquals("token", new TwitchModule().getApplicationAccessToken(identityProvider));
  }

  @SuppressWarnings("unchecked")
  private static HttpClient secretsClient() throws Exception {
    final HttpClient client = mock(HttpClient.class);
    final HttpResponse<String> response = mock(HttpResponse.class);
    when(response.body()).thenReturn(SECRETS_EXTENSION_RESPONSE);
    when(client.<String>send(any(), any())).thenReturn(response);
    return client;
  }
}
