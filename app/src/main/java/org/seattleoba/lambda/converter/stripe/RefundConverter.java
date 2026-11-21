package org.seattleoba.lambda.converter.stripe;

/** Converts Stripe SDK refunds to the stored {@code soba-dataformats} record. */
public class RefundConverter {
  /**
   * Copies the stored fields of a Stripe refund.
   *
   * @param input refund returned by the Stripe SDK
   * @return the record stored in DynamoDB
   */
  public static org.seattleoba.data.model.stripe.Refund convert(
      final com.stripe.model.Refund input) {
    return new org.seattleoba.data.model.stripe.Refund(
        input.getId(),
        input.getBalanceTransaction(),
        input.getCharge(),
        input.getPaymentIntent(),
        input.getAmount(),
        input.getCurrency(),
        input.getDescription(),
        input.getMetadata(),
        input.getReason(),
        input.getStatus());
  }
}
