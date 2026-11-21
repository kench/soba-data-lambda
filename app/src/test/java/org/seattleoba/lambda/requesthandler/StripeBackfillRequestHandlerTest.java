package org.seattleoba.lambda.requesthandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.amazonaws.services.lambda.runtime.Context;
import com.stripe.StripeClient;
import com.stripe.exception.ApiConnectionException;
import com.stripe.model.BalanceTransaction;
import com.stripe.model.Charge;
import com.stripe.model.Customer;
import com.stripe.model.HasId;
import com.stripe.model.PaymentIntent;
import com.stripe.model.Refund;
import com.stripe.model.StripeCollection;
import com.stripe.param.BalanceTransactionListParams;
import com.stripe.param.ChargeListParams;
import com.stripe.param.CustomerListParams;
import com.stripe.param.PaymentIntentListParams;
import com.stripe.param.RefundListParams;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.seattleoba.data.persistence.stripe.StripeBalanceTransactionStore;
import org.seattleoba.data.persistence.stripe.StripeChargeStore;
import org.seattleoba.data.persistence.stripe.StripeCustomerStore;
import org.seattleoba.data.persistence.stripe.StripePaymentIntentStore;
import org.seattleoba.data.persistence.stripe.StripeRefundStore;
import org.seattleoba.lambda.model.StripeBackfillRequest;
import org.seattleoba.lambda.model.StripeBackfillResult;

class StripeBackfillRequestHandlerTest {
  private final StripeClient stripeClient = mock(StripeClient.class, RETURNS_DEEP_STUBS);
  private final StripeBalanceTransactionStore balanceTransactionStore =
      mock(StripeBalanceTransactionStore.class);
  private final StripeChargeStore chargeStore = mock(StripeChargeStore.class);
  private final StripeCustomerStore customerStore = mock(StripeCustomerStore.class);
  private final StripePaymentIntentStore paymentIntentStore = mock(StripePaymentIntentStore.class);
  private final StripeRefundStore refundStore = mock(StripeRefundStore.class);
  private final StripeBackfillRequestHandler handler =
      new StripeBackfillRequestHandler(
          stripeClient,
          balanceTransactionStore,
          chargeStore,
          customerStore,
          paymentIntentStore,
          refundStore);

  private static <T extends HasId> StripeCollection<T> page(
      final boolean hasMore, final List<T> data) {
    final StripeCollection<T> collection = new StripeCollection<>();
    collection.setData(data);
    collection.setHasMore(hasMore);
    return collection;
  }

  private static Charge charge(final String id) {
    final Charge charge = new Charge();
    charge.setId(id);
    return charge;
  }

  private static StripeBackfillRequest chargesOnly() {
    return new StripeBackfillRequest(List.of("charge"), null, null, null);
  }

  @Test
  void backfillsEveryResourceTypeByDefault() throws Exception {
    final Customer customer = new Customer();
    customer.setId("cus_1");
    final PaymentIntent paymentIntent = new PaymentIntent();
    paymentIntent.setId("pi_1");
    final Refund refund = new Refund();
    refund.setId("re_1");
    final BalanceTransaction balanceTransaction = new BalanceTransaction();
    balanceTransaction.setId("txn_1");
    when(stripeClient.v1().customers().list(any(CustomerListParams.class)))
        .thenReturn(page(false, List.of(customer)));
    when(stripeClient.v1().paymentIntents().list(any(PaymentIntentListParams.class)))
        .thenReturn(page(false, List.of(paymentIntent)));
    when(stripeClient.v1().charges().list(any(ChargeListParams.class)))
        .thenReturn(page(false, List.of(charge("ch_1"), charge("ch_2"))));
    when(stripeClient.v1().refunds().list(any(RefundListParams.class)))
        .thenReturn(page(false, List.of(refund)));
    when(stripeClient.v1().balanceTransactions().list(any(BalanceTransactionListParams.class)))
        .thenReturn(page(false, List.of(balanceTransaction)));

    final StripeBackfillResult result = handler.handleRequest(null, null);

    assertTrue(result.complete());
    assertNull(result.nextRequest());
    assertEquals(0, result.failed());
    assertEquals(
        Map.of(
            "customer", 1, "payment-intent", 1, "charge", 2, "refund", 1, "balance-transaction", 1),
        result.stored());
    assertEquals(
        StripeBackfillRequestHandler.ALL_RESOURCE_TYPES, List.copyOf(result.stored().keySet()));
    verify(customerStore).updateCustomer(any());
    verify(paymentIntentStore).updatePaymentIntent(any());
    verify(chargeStore, times(2)).updateCharge(any());
    verify(refundStore).updateRefund(any());
    verify(balanceTransactionStore).updateBalanceTransaction(any());
  }

  @Test
  void emptyResourceTypeListMeansAll() throws Exception {
    final StripeBackfillResult result =
        handler.handleRequest(new StripeBackfillRequest(List.of(), null, null, null), null);

    assertTrue(result.complete());
    assertEquals(StripeBackfillRequestHandler.ALL_RESOURCE_TYPES.size(), result.stored().size());
  }

  @Test
  void followsPaginationCursor() throws Exception {
    when(stripeClient.v1().charges().list(any(ChargeListParams.class)))
        .thenReturn(page(true, List.of(charge("ch_3"), charge("ch_2"))))
        .thenReturn(page(false, List.of(charge("ch_1"))));

    final StripeBackfillResult result = handler.handleRequest(chargesOnly(), null);

    assertTrue(result.complete());
    assertEquals(Map.of("charge", 3), result.stored());
    final ArgumentCaptor<ChargeListParams> params = ArgumentCaptor.forClass(ChargeListParams.class);
    verify(stripeClient.v1().charges(), times(2)).list(params.capture());
    assertNull(params.getAllValues().get(0).getStartingAfter());
    assertEquals(100L, params.getAllValues().get(0).getLimit());
    assertNull(params.getAllValues().get(0).getCreated());
    assertEquals("ch_2", params.getAllValues().get(1).getStartingAfter());
  }

  @Test
  void stopsOnEmptyPageEvenIfStripeSaysMore() throws Exception {
    when(stripeClient.v1().charges().list(any(ChargeListParams.class)))
        .thenReturn(page(true, List.of()));

    final StripeBackfillResult result = handler.handleRequest(chargesOnly(), null);

    assertTrue(result.complete());
    verify(stripeClient.v1().charges(), times(1)).list(any(ChargeListParams.class));
  }

  @Test
  void treatsMissingDataAsEmptyPage() throws Exception {
    when(stripeClient.v1().charges().list(any(ChargeListParams.class)))
        .thenReturn(page(true, null));

    final StripeBackfillResult result = handler.handleRequest(chargesOnly(), null);

    assertTrue(result.complete());
    assertEquals(Map.of("charge", 0), result.stored());
  }

  @Test
  void passesCreatedRangeAndStartingAfterToEveryResourceType() throws Exception {
    final StripeBackfillResult result =
        handler.handleRequest(
            new StripeBackfillRequest(
                StripeBackfillRequestHandler.ALL_RESOURCE_TYPES, 100L, 200L, "cus_9"),
            null);
    assertTrue(result.complete());

    final ArgumentCaptor<CustomerListParams> customerParams =
        ArgumentCaptor.forClass(CustomerListParams.class);
    verify(stripeClient.v1().customers()).list(customerParams.capture());
    assertEquals("cus_9", customerParams.getValue().getStartingAfter());
    assertEquals(
        100L, ((CustomerListParams.Created) customerParams.getValue().getCreated()).getGte());
    assertEquals(
        200L, ((CustomerListParams.Created) customerParams.getValue().getCreated()).getLte());

    // The cursor only applies to the first resource type.
    final ArgumentCaptor<PaymentIntentListParams> paymentIntentParams =
        ArgumentCaptor.forClass(PaymentIntentListParams.class);
    verify(stripeClient.v1().paymentIntents()).list(paymentIntentParams.capture());
    assertNull(paymentIntentParams.getValue().getStartingAfter());
    assertEquals(
        100L,
        ((PaymentIntentListParams.Created) paymentIntentParams.getValue().getCreated()).getGte());

    final ArgumentCaptor<ChargeListParams> chargeParams =
        ArgumentCaptor.forClass(ChargeListParams.class);
    verify(stripeClient.v1().charges()).list(chargeParams.capture());
    assertEquals(200L, ((ChargeListParams.Created) chargeParams.getValue().getCreated()).getLte());

    final ArgumentCaptor<RefundListParams> refundParams =
        ArgumentCaptor.forClass(RefundListParams.class);
    verify(stripeClient.v1().refunds()).list(refundParams.capture());
    assertEquals(100L, ((RefundListParams.Created) refundParams.getValue().getCreated()).getGte());

    final ArgumentCaptor<BalanceTransactionListParams> balanceTransactionParams =
        ArgumentCaptor.forClass(BalanceTransactionListParams.class);
    verify(stripeClient.v1().balanceTransactions()).list(balanceTransactionParams.capture());
    assertEquals(
        200L,
        ((BalanceTransactionListParams.Created) balanceTransactionParams.getValue().getCreated())
            .getLte());
  }

  @Test
  void passesCursorWithoutCreatedRange() throws Exception {
    for (final String resourceType : StripeBackfillRequestHandler.ALL_RESOURCE_TYPES) {
      handler.handleRequest(
          new StripeBackfillRequest(List.of(resourceType), null, null, "x_1"), null);
    }

    final ArgumentCaptor<BalanceTransactionListParams> balanceTransactionParams =
        ArgumentCaptor.forClass(BalanceTransactionListParams.class);
    verify(stripeClient.v1().balanceTransactions()).list(balanceTransactionParams.capture());
    assertEquals("x_1", balanceTransactionParams.getValue().getStartingAfter());
    assertNull(balanceTransactionParams.getValue().getCreated());

    final ArgumentCaptor<ChargeListParams> chargeParams =
        ArgumentCaptor.forClass(ChargeListParams.class);
    verify(stripeClient.v1().charges()).list(chargeParams.capture());
    assertEquals("x_1", chargeParams.getValue().getStartingAfter());

    final ArgumentCaptor<PaymentIntentListParams> paymentIntentParams =
        ArgumentCaptor.forClass(PaymentIntentListParams.class);
    verify(stripeClient.v1().paymentIntents()).list(paymentIntentParams.capture());
    assertEquals("x_1", paymentIntentParams.getValue().getStartingAfter());

    final ArgumentCaptor<RefundListParams> refundParams =
        ArgumentCaptor.forClass(RefundListParams.class);
    verify(stripeClient.v1().refunds()).list(refundParams.capture());
    assertEquals("x_1", refundParams.getValue().getStartingAfter());
  }

  @Test
  void onlyLowerBoundIsAllowed() throws Exception {
    handler.handleRequest(new StripeBackfillRequest(List.of("charge"), 100L, null, null), null);
    handler.handleRequest(new StripeBackfillRequest(List.of("refund"), null, 200L, null), null);

    final ArgumentCaptor<ChargeListParams> chargeParams =
        ArgumentCaptor.forClass(ChargeListParams.class);
    verify(stripeClient.v1().charges()).list(chargeParams.capture());
    final ChargeListParams.Created created =
        (ChargeListParams.Created) chargeParams.getValue().getCreated();
    assertEquals(100L, created.getGte());
    assertNull(created.getLte());

    final ArgumentCaptor<RefundListParams> refundParams =
        ArgumentCaptor.forClass(RefundListParams.class);
    verify(stripeClient.v1().refunds()).list(refundParams.capture());
    assertEquals(200L, ((RefundListParams.Created) refundParams.getValue().getCreated()).getLte());
  }

  @Test
  void rejectsUnknownResourceTypeBeforeCallingStripe() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            handler.handleRequest(
                new StripeBackfillRequest(List.of("charge", "invoice"), null, null, null), null));
    verifyNoInteractions(chargeStore);
  }

  @Test
  void countsStoreFailuresAndKeepsGoing() throws Exception {
    when(stripeClient.v1().charges().list(any(ChargeListParams.class)))
        .thenReturn(page(false, List.of(charge("ch_2"), charge("ch_1"))));
    doThrow(new IllegalStateException("throttled"))
        .doNothing()
        .when(chargeStore)
        .updateCharge(any());

    final StripeBackfillResult result = handler.handleRequest(chargesOnly(), null);

    assertTrue(result.complete());
    assertEquals(1, result.failed());
    assertEquals(Map.of("charge", 1), result.stored());
    verify(chargeStore, times(2)).updateCharge(any());
  }

  @Test
  void returnsResumeRequestWhenStripeFails() throws Exception {
    when(stripeClient.v1().customers().list(any(CustomerListParams.class)))
        .thenReturn(page(false, List.of()));
    when(stripeClient.v1().paymentIntents().list(any(PaymentIntentListParams.class)))
        .thenReturn(page(false, List.of()));
    when(stripeClient.v1().charges().list(any(ChargeListParams.class)))
        .thenReturn(page(true, List.of(charge("ch_5"))))
        .thenThrow(new ApiConnectionException("timeout"));

    final StripeBackfillResult result =
        handler.handleRequest(new StripeBackfillRequest(null, 100L, 200L, null), null);

    assertFalse(result.complete());
    assertEquals(Map.of("customer", 0, "payment-intent", 0, "charge", 1), result.stored());
    assertEquals(
        new StripeBackfillRequest(
            List.of("charge", "refund", "balance-transaction"), 100L, 200L, "ch_5"),
        result.nextRequest());
    verify(stripeClient.v1().refunds(), never()).list(any(RefundListParams.class));
  }

  @Test
  void stopsBeforeLambdaTimesOut() throws Exception {
    final Context context = mock(Context.class);
    when(context.getRemainingTimeInMillis()).thenReturn(120_000).thenReturn(30_000);
    when(stripeClient.v1().charges().list(any(ChargeListParams.class)))
        .thenReturn(page(true, List.of(charge("ch_2"))));

    final StripeBackfillResult result = handler.handleRequest(chargesOnly(), context);

    assertFalse(result.complete());
    assertEquals(Map.of("charge", 1), result.stored());
    assertEquals(
        new StripeBackfillRequest(List.of("charge"), null, null, "ch_2"), result.nextRequest());
    verify(stripeClient.v1().charges(), times(1)).list(any(ChargeListParams.class));
  }

  @Test
  void resumedRunKeepsOriginalCursorIfNoProgress() throws Exception {
    final Context context = mock(Context.class);
    when(context.getRemainingTimeInMillis()).thenReturn(0);

    final StripeBackfillResult result =
        handler.handleRequest(
            new StripeBackfillRequest(List.of("refund"), null, null, "re_7"), context);

    assertFalse(result.complete());
    assertEquals(
        new StripeBackfillRequest(List.of("refund"), null, null, "re_7"), result.nextRequest());
  }
}
