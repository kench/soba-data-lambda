package org.seattleoba.lambda.requesthandler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.stripe.StripeClient;
import com.stripe.exception.StripeException;
import com.stripe.model.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.seattleoba.lambda.converter.stripe.*;
import org.seattleoba.lambda.model.StripeDataSyncRequest;
import org.seattleoba.data.persistence.stripe.StripeBalanceTransactionStore;
import org.seattleoba.data.persistence.stripe.StripeChargeStore;
import org.seattleoba.data.persistence.stripe.StripeCustomerStore;
import org.seattleoba.data.persistence.stripe.StripePaymentIntentStore;
import org.seattleoba.data.persistence.stripe.StripeRefundStore;

import javax.inject.Inject;

public class StripeDataSyncRequestHandler implements RequestHandler<StripeDataSyncRequest, Void> {
    private static final Logger LOG = LogManager.getLogger(StripeDataSyncRequestHandler.class);

    private static final String BALANCE_TRANSACTION_RESOURCE_TYPE = "balance-transaction";
    private static final String CHARGE_RESOURCE_TYPE = "charge";
    private static final String CUSTOMER_RESOURCE_TYPE = "customer";
    private static final String PAYMENT_INTENT_RESOURCE_TYPE = "payment-intent";
    private static final String REFUND_RESOURCE_TYPE = "refund";

    private final StripeClient stripeClient;
    private final StripeBalanceTransactionStore stripeBalanceTransactionStore;
    private final StripeChargeStore stripeChargeStore;
    private final StripeCustomerStore stripeCustomerStore;
    private final StripePaymentIntentStore stripePaymentIntentStore;
    private final StripeRefundStore stripeRefundStore;

    @Inject
    public StripeDataSyncRequestHandler(
            final StripeClient stripeClient,
            final StripeBalanceTransactionStore stripeBalanceTransactionStore,
            final StripeChargeStore stripeChargeStore,
            final StripeCustomerStore stripeCustomerStore,
            final StripePaymentIntentStore stripePaymentIntentStore,
            final StripeRefundStore stripeRefundStore) {
        this.stripeClient = stripeClient;
        this.stripeBalanceTransactionStore = stripeBalanceTransactionStore;
        this.stripeChargeStore = stripeChargeStore;
        this.stripeCustomerStore = stripeCustomerStore;
        this.stripePaymentIntentStore = stripePaymentIntentStore;
        this.stripeRefundStore = stripeRefundStore;
    }

    @Override
    public Void handleRequest(final StripeDataSyncRequest input, final Context context) {
        switch (input.resourceType()) {
            case BALANCE_TRANSACTION_RESOURCE_TYPE -> {
                final BalanceTransaction balanceTransaction;
                try {
                    balanceTransaction = stripeClient.v1().balanceTransactions().retrieve(input.id());
                    stripeBalanceTransactionStore.updateBalanceTransaction(BalanceTransactionConverter.convert(balanceTransaction));
                } catch (final StripeException exception) {
                    LOG.error("Unable to retrieve balance transaction {} from Stripe", input.id(), exception);
                }
            }
            case CHARGE_RESOURCE_TYPE -> {
                final Charge charge;
                try {
                    charge = stripeClient.v1().charges().retrieve(input.id());
                    stripeChargeStore.updateCharge(ChargeConverter.convert(charge));
                } catch (final StripeException exception) {
                    LOG.error("Unable to retrieve charge {} from Stripe", input.id(), exception);
                }
            }
            case CUSTOMER_RESOURCE_TYPE -> {
                final Customer customer;
                try {
                    customer = stripeClient.v1().customers().retrieve(input.id());
                    stripeCustomerStore.updateCustomer(CustomerConverter.convert(customer));
                } catch (final StripeException exception) {
                    LOG.error("Unable to retrieve customer {} from Stripe", input.id(), exception);
                }
            }
            case PAYMENT_INTENT_RESOURCE_TYPE -> {
                final PaymentIntent paymentIntent;
                try {
                    paymentIntent = stripeClient.v1().paymentIntents().retrieve(input.id());
                    stripePaymentIntentStore.updatePaymentIntent(PaymentIntentConverter.convert(paymentIntent));
                } catch (final StripeException exception) {
                    LOG.error("Unable to retrieve payment intent {} from Stripe", input.id(), exception);
                }
            }
            case REFUND_RESOURCE_TYPE -> {
                final Refund refund;
                try {
                    refund = stripeClient.v1().refunds().retrieve(input.id());
                    stripeRefundStore.updateRefund(RefundConverter.convert(refund));
                } catch (final StripeException exception) {
                    LOG.error("Unable to retrieve refund {} from Stripe", input.id(), exception);
                }
            }
            default -> throw new IllegalArgumentException(String.format("Unknown resource type", input.resourceType()));
        }
        return null;
    }
}
