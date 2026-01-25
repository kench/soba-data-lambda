package org.seattleoba.lambda.dagger;

import dagger.Module;
import dagger.Provides;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Named;
import javax.inject.Singleton;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

@Module
public class TwitchModule {
    private static final Logger LOG = LogManager.getLogger(TwitchModule.class);

    @Provides
    @Singleton
    @Named("clientId")
    public String providesClientId() {
        return System.getenv("CLIENT_ID");
    }

    @Provides
    @Singleton
    public HttpClient providesHttpClient() {
        return HttpClient.newHttpClient();
    }

    @Provides
    @Singleton
    @Named("clientSecret")
    public String providesClientSecret(final HttpClient client) {
        try {
            final String secretName = System.getenv("CLIENT_SECRET_ARN");;
            String endpoint = "http://localhost:2773/secretsmanager/get?secretId=" + secretName;

            final HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .header("X-Aws-Parameters-Secrets-Token", System.getenv("AWS_SESSION_TOKEN"))
                    .GET()
                    .build();

            final HttpResponse<String> response = client.send(request,
                    HttpResponse.BodyHandlers.ofString());

            String secret = response.body();
            secret = secret.substring(secret.indexOf("SecretString") + 15);
            secret = secret.substring(0, secret.indexOf("\""));

            LOG.info("Successfully retrieved Twitch client secret");
            return secret;

        } catch (final Exception exception) {
            LOG.error("Unable to retrieve Twitch client secret", exception);
            throw new IllegalStateException(exception);
        }
    }
}
