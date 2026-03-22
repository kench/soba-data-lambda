package org.seattleoba.lambda.requesthandler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.stripe.StripeClient;
import com.stripe.exception.StripeException;
import com.stripe.model.BalanceTransaction;
import com.stripe.model.PaymentIntent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.seattleoba.lambda.converter.stripe.BalanceTransactionConverter;
import org.seattleoba.lambda.converter.stripe.PaymentIntentConverter;
import org.seattleoba.lambda.model.StripeDataSyncRequest;
import org.seattleoba.data.persistence.stripe.StripeBalanceTransactionStore;
import org.seattleoba.data.persistence.stripe.StripePaymentIntentStore;

import javax.inject.Inject;

public class StripeDataSyncRequestHandler implements RequestHandler<StripeDataSyncRequest, Void> {
    private static final Logger LOG = LogManager.getLogger(StripeDataSyncRequestHandler.class);

    private static final String BALANCE_TRANSACTION_RESOURCE_TYPE = "balance-transaction";
    private static final String PAYMENT_INTENT_RESOURCE_TYPE = "payment-intent";

    private final StripeClient stripeClient;
    private final StripeBalanceTransactionStore stripeBalanceTransactionStore;
    private final StripePaymentIntentStore stripePaymentIntentStore;

    @Inject
    public StripeDataSyncRequestHandler(
            final StripeClient stripeClient,
            final StripeBalanceTransactionStore stripeBalanceTransactionStore,
            final StripePaymentIntentStore stripePaymentIntentStore) {
        this.stripeClient = stripeClient;
        this.stripeBalanceTransactionStore = stripeBalanceTransactionStore;
        this.stripePaymentIntentStore = stripePaymentIntentStore;
    }

    @Override
    public Void handleRequest(final StripeDataSyncRequest input, final Context context) {
        if (input.resourceType().equals(BALANCE_TRANSACTION_RESOURCE_TYPE)) {
            final BalanceTransaction balanceTransaction;
            try {
                balanceTransaction = stripeClient.v1().balanceTransactions().retrieve(input.id());
                stripeBalanceTransactionStore.updateBalanceTransaction(BalanceTransactionConverter.convert(balanceTransaction));
            } catch (final StripeException exception) {
                LOG.error("Unable to retrieve balance transaction {} from Stripe", input.id(), exception);
            }
        } else if (input.resourceType().equals(PAYMENT_INTENT_RESOURCE_TYPE)) {
            final PaymentIntent paymentIntent;
            try {
                paymentIntent = stripeClient.v1().paymentIntents().retrieve(input.id());
                stripePaymentIntentStore.updatePaymentIntent(PaymentIntentConverter.convert(paymentIntent));
            } catch (final StripeException exception) {
                LOG.error("Unable to retrieve payment intent {} from Stripe", input.id(), exception);
            }
        }
        return null;
    }
}
