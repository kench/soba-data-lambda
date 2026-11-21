package org.seattleoba.lambda.dagger;

import com.github.twitch4j.TwitchClient;
import com.github.twitch4j.TwitchClientBuilder;
import com.github.twitch4j.auth.providers.TwitchIdentityProvider;
import dagger.Module;
import dagger.Provides;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import javax.inject.Named;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.seattleoba.lambda.twitch.TwitchDataProvider;

/** Dagger module that provides the Twitch API clients and credentials. */
@Module
public class TwitchModule {
  private static final Logger LOG = LogManager.getLogger(TwitchModule.class);
  private static final String REDIRECT_URL = "https://services.seattleoba.org/";

  /**
   * Provides the Twitch client ID from the {@code CLIENT_ID} environment variable.
   *
   * @return the client ID
   */
  @Provides
  @Singleton
  @Named("clientId")
  public String providesClientId() {
    return System.getenv("CLIENT_ID");
  }

  /**
   * Provides the HTTP client used to reach the Secrets Manager extension.
   *
   * @return the client
   */
  @Provides
  @Singleton
  public HttpClient providesHttpClient() {
    return HttpClient.newHttpClient();
  }

  /**
   * Fetches the Twitch client secret from the AWS Parameters and Secrets Lambda extension.
   *
   * @param client HTTP client
   * @return the client secret
   * @throws IllegalStateException if the secret can't be retrieved
   */
  @Provides
  @Singleton
  @Named("clientSecret")
  public String providesClientSecret(final HttpClient client) {
    try {
      final String secretName = System.getenv("CLIENT_SECRET_ARN");
      String endpoint = "http://localhost:2773/secretsmanager/get?secretId=" + secretName;

      final HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(endpoint))
              .header("X-Aws-Parameters-Secrets-Token", System.getenv("AWS_SESSION_TOKEN"))
              .GET()
              .build();

      final HttpResponse<String> response =
          client.send(request, HttpResponse.BodyHandlers.ofString());

      String secret = response.body();
      secret = secret.substring(secret.indexOf("SecretString") + 15);
      secret = secret.substring(0, secret.indexOf("\""));

      return secret;

    } catch (final Exception exception) {
      LOG.error("Unable to retrieve Twitch client secret", exception);
      throw new IllegalStateException(exception);
    }
  }

  /**
   * Provides the Twitch client with Helix enabled.
   *
   * @param clientId Twitch client ID
   * @param clientSecret Twitch client secret
   * @return the client
   */
  @Provides
  @Singleton
  public TwitchClient providesTwitchClient(
      @Named("clientId") final String clientId, @Named("clientSecret") final String clientSecret) {
    return TwitchClientBuilder.builder()
        .withEnableHelix(true)
        .withClientId(clientId)
        .withClientSecret(clientSecret)
        .build();
  }

  /**
   * Provides the Twitch API wrapper.
   *
   * @param twitchClient Twitch client
   * @return the wrapper
   */
  @Provides
  @Singleton
  public TwitchDataProvider providesTwitchDataProvider(final TwitchClient twitchClient) {
    return new TwitchDataProvider(twitchClient);
  }

  /**
   * Provides the Twitch identity provider used for app access tokens.
   *
   * @param clientId Twitch client ID
   * @param clientSecret Twitch client secret
   * @return the provider
   */
  @Provides
  @Singleton
  public TwitchIdentityProvider providesTwitchIdentityProvider(
      @Named("clientId") final String clientId, @Named("clientSecret") final String clientSecret) {
    return new TwitchIdentityProvider(clientId, clientSecret, REDIRECT_URL);
  }

  /**
   * Provides a Twitch app access token.
   *
   * @param twitchIdentityProvider Twitch identity provider
   * @return the access token
   */
  @Provides
  @Singleton
  @Named("accessToken")
  public String getApplicationAccessToken(final TwitchIdentityProvider twitchIdentityProvider) {
    return twitchIdentityProvider.getAppAccessToken().getAccessToken();
  }
}
