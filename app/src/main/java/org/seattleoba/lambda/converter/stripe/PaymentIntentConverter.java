package org.seattleoba.lambda.converter.stripe;

/** Converts Stripe SDK payment intents to the stored {@code soba-dataformats} record. */
public final class PaymentIntentConverter {
  /**
   * Copies the stored fields of a Stripe payment intent.
   *
   * @param input payment intent returned by the Stripe SDK
   * @return the record stored in DynamoDB
   */
  public static org.seattleoba.data.model.stripe.PaymentIntent convert(
      final com.stripe.model.PaymentIntent input) {
    return new org.seattleoba.data.model.stripe.PaymentIntent(
        input.getId(),
        input.getLatestCharge(),
        input.getCustomer(),
        input.getDescription(),
        input.getAmount(),
        input.getCurrency(),
        input.getMetadata(),
        input.getPaymentMethod(),
        input.getReceiptEmail(),
        input.getStatus(),
        input.getCreated(),
        input.getApplication(),
        input.getApplicationFeeAmount());
  }
}
