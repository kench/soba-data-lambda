package org.seattleoba.lambda.requesthandler;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.stripe.StripeClient;
import com.stripe.exception.ApiConnectionException;
import com.stripe.model.BalanceTransaction;
import com.stripe.model.Charge;
import com.stripe.model.Customer;
import com.stripe.model.PaymentIntent;
import com.stripe.model.Refund;
import org.junit.jupiter.api.Test;
import org.seattleoba.data.persistence.stripe.StripeBalanceTransactionStore;
import org.seattleoba.data.persistence.stripe.StripeChargeStore;
import org.seattleoba.data.persistence.stripe.StripeCustomerStore;
import org.seattleoba.data.persistence.stripe.StripePaymentIntentStore;
import org.seattleoba.data.persistence.stripe.StripeRefundStore;
import org.seattleoba.lambda.model.StripeDataSyncRequest;

class StripeDataSyncRequestHandlerTest {
  private final StripeClient stripeClient = mock(StripeClient.class, RETURNS_DEEP_STUBS);
  private final StripeBalanceTransactionStore balanceTransactionStore =
      mock(StripeBalanceTransactionStore.class);
  private final StripeChargeStore chargeStore = mock(StripeChargeStore.class);
  private final StripeCustomerStore customerStore = mock(StripeCustomerStore.class);
  private final StripePaymentIntentStore paymentIntentStore = mock(StripePaymentIntentStore.class);
  private final StripeRefundStore refundStore = mock(StripeRefundStore.class);
  private final StripeDataSyncRequestHandler handler =
      new StripeDataSyncRequestHandler(
          stripeClient,
          balanceTransactionStore,
          chargeStore,
          customerStore,
          paymentIntentStore,
          refundStore);

  @Test
  void syncsBalanceTransaction() throws Exception {
    final BalanceTransaction balanceTransaction = new BalanceTransaction();
    balanceTransaction.setId("txn_1");
    when(stripeClient.v1().balanceTransactions().retrieve("txn_1")).thenReturn(balanceTransaction);

    handler.sync("balance-transaction", "txn_1");

    verify(balanceTransactionStore).updateBalanceTransaction(any());
  }

  @Test
  void syncsCharge() throws Exception {
    final Charge charge = new Charge();
    charge.setId("ch_1");
    when(stripeClient.v1().charges().retrieve("ch_1")).thenReturn(charge);

    handler.sync("charge", "ch_1");

    verify(chargeStore).updateCharge(any());
  }

  @Test
  void syncsCustomer() throws Exception {
    final Customer customer = new Customer();
    customer.setId("cus_1");
    when(stripeClient.v1().customers().retrieve("cus_1")).thenReturn(customer);

    handler.sync("customer", "cus_1");

    verify(customerStore).updateCustomer(any());
  }

  @Test
  void syncsPaymentIntent() throws Exception {
    final PaymentIntent paymentIntent = new PaymentIntent();
    paymentIntent.setId("pi_1");
    when(stripeClient.v1().paymentIntents().retrieve("pi_1")).thenReturn(paymentIntent);

    handler.sync("payment-intent", "pi_1");

    verify(paymentIntentStore).updatePaymentIntent(any());
  }

  @Test
  void syncsRefund() throws Exception {
    final Refund refund = new Refund();
    refund.setId("re_1");
    when(stripeClient.v1().refunds().retrieve("re_1")).thenReturn(refund);

    handler.sync("refund", "re_1");

    verify(refundStore).updateRefund(any());
  }

  @Test
  void rejectsUnknownResourceType() {
    assertThrows(IllegalArgumentException.class, () -> handler.sync("invoice", "in_1"));
  }

  @Test
  void handleRequestLogsStripeFailures() throws Exception {
    when(stripeClient.v1().charges().retrieve("ch_1"))
        .thenThrow(new ApiConnectionException("timeout"));

    assertNull(handler.handleRequest(new StripeDataSyncRequest("charge", "ch_1"), null));
    verifyNoInteractions(chargeStore);
  }

  @Test
  void handleRequestSyncsResource() throws Exception {
    final Customer customer = new Customer();
    customer.setId("cus_1");
    when(stripeClient.v1().customers().retrieve("cus_1")).thenReturn(customer);

    handler.handleRequest(new StripeDataSyncRequest("customer", "cus_1"), null);

    verify(customerStore).updateCustomer(any());
  }
}
