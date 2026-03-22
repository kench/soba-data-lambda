package org.seattleoba.lambda.dagger;

import com.stripe.StripeClient;
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
public class StripeModule {
    private static final Logger LOG = LogManager.getLogger(StripeModule.class);
    private static final String STRIPE_SECRET_KEY = "STRIPE_SECRET_KEY";

    @Provides
    @Singleton
    public HttpClient providesHttpClient() {
        return HttpClient.newHttpClient();
    }

    @Provides
    @Singleton
    @Named(STRIPE_SECRET_KEY)
    public String providesStripeSecretKey(final HttpClient client) {
        try {
            final String secretName = System.getenv("STRIPE_SECRET_KEY_ARN");
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

            return secret;

        } catch (final Exception exception) {
            LOG.error("Unable to retrieve Stripe secret key", exception);
            throw new IllegalStateException(exception);
        }
    }

    @Provides
    @Singleton
    public StripeClient providesStripeClient(@Named("STRIPE_SECRET_KEY") final String secretKey) {
        return new StripeClient(secretKey);
    }
}
