package org.seattleoba.lambda.converter.stripe;

/** Converts Stripe SDK charges to the stored {@code soba-dataformats} record. */
public class ChargeConverter {
  /**
   * Copies the stored fields of a Stripe charge.
   *
   * @param input charge returned by the Stripe SDK
   * @return the record stored in DynamoDB
   */
  public static org.seattleoba.data.model.stripe.Charge convert(
      final com.stripe.model.Charge input) {
    return new org.seattleoba.data.model.stripe.Charge(
        input.getId(),
        input.getAmount(),
        input.getBalanceTransaction(),
        input.getCaptured(),
        input.getCreated(),
        input.getCurrency(),
        input.getCustomer(),
        input.getDescription(),
        input.getDisputed(),
        input.getFailureBalanceTransaction(),
        input.getMetadata(),
        input.getPaymentIntent(),
        input.getPaymentMethod(),
        input.getReceiptUrl(),
        input.getRefunded(),
        input.getStatus());
  }
}
