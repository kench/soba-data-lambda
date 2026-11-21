package org.seattleoba.lambda.requesthandler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.stripe.StripeClient;
import com.stripe.exception.StripeException;
import com.stripe.model.BalanceTransaction;
import com.stripe.model.Charge;
import com.stripe.model.Customer;
import com.stripe.model.PaymentIntent;
import com.stripe.model.Refund;
import javax.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.seattleoba.data.persistence.stripe.StripeBalanceTransactionStore;
import org.seattleoba.data.persistence.stripe.StripeChargeStore;
import org.seattleoba.data.persistence.stripe.StripeCustomerStore;
import org.seattleoba.data.persistence.stripe.StripePaymentIntentStore;
import org.seattleoba.data.persistence.stripe.StripeRefundStore;
import org.seattleoba.lambda.converter.stripe.BalanceTransactionConverter;
import org.seattleoba.lambda.converter.stripe.ChargeConverter;
import org.seattleoba.lambda.converter.stripe.CustomerConverter;
import org.seattleoba.lambda.converter.stripe.PaymentIntentConverter;
import org.seattleoba.lambda.converter.stripe.RefundConverter;
import org.seattleoba.lambda.model.StripeDataSyncRequest;

/** Fetches a Stripe object by ID and stores the latest copy in its DynamoDB table. */
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

  /**
   * Creates the handler.
   *
   * @param stripeClient Stripe API client
   * @param stripeBalanceTransactionStore store for balance transactions
   * @param stripeChargeStore store for charges
   * @param stripeCustomerStore store for customers
   * @param stripePaymentIntentStore store for payment intents
   * @param stripeRefundStore store for refunds
   */
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
    try {
      sync(input.resourceType(), input.id());
    } catch (final StripeException exception) {
      LOG.error(
          "Unable to retrieve {} {} from Stripe", input.resourceType(), input.id(), exception);
    }
    return null;
  }

  /**
   * Fetches the latest copy of a Stripe resource and writes it to its DynamoDB table.
   *
   * @param resourceType one of the hyphenated resource types, e.g. "payment-intent"
   * @param id Stripe object ID
   * @throws StripeException if the resource cannot be retrieved from Stripe
   * @throws IllegalArgumentException if the resource type is not supported
   */
  public void sync(final String resourceType, final String id) throws StripeException {
    switch (resourceType) {
      case BALANCE_TRANSACTION_RESOURCE_TYPE -> {
        final BalanceTransaction balanceTransaction =
            stripeClient.v1().balanceTransactions().retrieve(id);
        stripeBalanceTransactionStore.updateBalanceTransaction(
            BalanceTransactionConverter.convert(balanceTransaction));
      }
      case CHARGE_RESOURCE_TYPE -> {
        final Charge charge = stripeClient.v1().charges().retrieve(id);
        stripeChargeStore.updateCharge(ChargeConverter.convert(charge));
      }
      case CUSTOMER_RESOURCE_TYPE -> {
        final Customer customer = stripeClient.v1().customers().retrieve(id);
        stripeCustomerStore.updateCustomer(CustomerConverter.convert(customer));
      }
      case PAYMENT_INTENT_RESOURCE_TYPE -> {
        final PaymentIntent paymentIntent = stripeClient.v1().paymentIntents().retrieve(id);
        stripePaymentIntentStore.updatePaymentIntent(PaymentIntentConverter.convert(paymentIntent));
      }
      case REFUND_RESOURCE_TYPE -> {
        final Refund refund = stripeClient.v1().refunds().retrieve(id);
        stripeRefundStore.updateRefund(RefundConverter.convert(refund));
      }
      default ->
          throw new IllegalArgumentException(
              String.format("Unknown resource type %s", resourceType));
    }
  }
}
