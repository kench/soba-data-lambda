package org.seattleoba.lambda.converter.stripe;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.stripe.model.BalanceTransaction;
import com.stripe.model.Charge;
import com.stripe.model.Customer;
import com.stripe.model.PaymentIntent;
import com.stripe.model.Refund;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for the Stripe SDK to stored record converters. */
class ConvertersTest {
  private static final Map<String, String> METADATA = Map.of("order", "TTE25065088");

  @Test
  void convertsBalanceTransaction() {
    final BalanceTransaction input = new BalanceTransaction();
    input.setId("txn_1");
    input.setAmount(999L);
    input.setAvailableOn(1_700_000_100L);
    input.setCreated(1_700_000_000L);
    input.setCurrency("usd");
    input.setDescription("Ticket");
    input.setFee(59L);
    input.setNet(940L);
    input.setSource("ch_1");
    input.setStatus("available");
    input.setType("charge");

    assertEquals(
        new org.seattleoba.data.model.stripe.BalanceTransaction(
            "txn_1",
            999L,
            1_700_000_100L,
            1_700_000_000L,
            "usd",
            "Ticket",
            59L,
            940L,
            "ch_1",
            "available",
            "charge"),
        BalanceTransactionConverter.convert(input));
  }

  @Test
  void convertsCharge() {
    final Charge input = new Charge();
    input.setId("ch_1");
    input.setAmount(999L);
    input.setBalanceTransaction("txn_1");
    input.setCaptured(true);
    input.setCreated(1_700_000_000L);
    input.setCurrency("usd");
    input.setCustomer("cus_1");
    input.setDescription("Ticket");
    input.setDisputed(false);
    input.setFailureBalanceTransaction("txn_2");
    input.setMetadata(METADATA);
    input.setPaymentIntent("pi_1");
    input.setPaymentMethod("pm_1");
    input.setReceiptUrl("https://pay.stripe.com/receipts/1");
    input.setRefunded(false);
    input.setStatus("succeeded");

    assertEquals(
        new org.seattleoba.data.model.stripe.Charge(
            "ch_1",
            999L,
            "txn_1",
            true,
            1_700_000_000L,
            "usd",
            "cus_1",
            "Ticket",
            false,
            "txn_2",
            METADATA,
            "pi_1",
            "pm_1",
            "https://pay.stripe.com/receipts/1",
            false,
            "succeeded"),
        ChargeConverter.convert(input));
  }

  @Test
  void convertsCustomer() {
    final Customer input = new Customer();
    input.setId("cus_1");
    input.setEmail("soba@example.com");
    input.setName("SeattleOBA");

    assertEquals(
        new org.seattleoba.data.model.stripe.Customer("cus_1", "soba@example.com", "SeattleOBA"),
        CustomerConverter.convert(input));
  }

  @Test
  void convertsPaymentIntent() {
    final PaymentIntent input = new PaymentIntent();
    input.setId("pi_1");
    input.setLatestCharge("ch_1");
    input.setCustomer("cus_1");
    input.setDescription("TTE25065088");
    input.setAmount(999L);
    input.setCurrency("usd");
    input.setMetadata(METADATA);
    input.setPaymentMethod("pm_1");
    input.setReceiptEmail("soba@example.com");
    input.setStatus("succeeded");
    input.setCreated(1_700_000_000L);
    input.setApplication("ca_1");
    input.setApplicationFeeAmount(10L);

    assertEquals(
        new org.seattleoba.data.model.stripe.PaymentIntent(
            "pi_1",
            "ch_1",
            "cus_1",
            "TTE25065088",
            999L,
            "usd",
            METADATA,
            "pm_1",
            "soba@example.com",
            "succeeded",
            1_700_000_000L,
            "ca_1",
            10L),
        PaymentIntentConverter.convert(input));
  }

  @Test
  void convertsRefund() {
    final Refund input = new Refund();
    input.setId("re_1");
    input.setBalanceTransaction("txn_3");
    input.setCharge("ch_1");
    input.setPaymentIntent("pi_1");
    input.setAmount(999L);
    input.setCurrency("usd");
    input.setDescription("Refund");
    input.setMetadata(METADATA);
    input.setReason("requested_by_customer");
    input.setStatus("succeeded");

    assertEquals(
        new org.seattleoba.data.model.stripe.Refund(
            "re_1",
            "txn_3",
            "ch_1",
            "pi_1",
            999L,
            "usd",
            "Refund",
            METADATA,
            "requested_by_customer",
            "succeeded"),
        RefundConverter.convert(input));
  }
}
