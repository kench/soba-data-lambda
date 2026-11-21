package org.seattleoba.lambda.converter.stripe;

/** Converts Stripe SDK customers to the stored {@code soba-dataformats} record. */
public class CustomerConverter {
  /**
   * Copies the stored fields of a Stripe customer.
   *
   * @param input customer returned by the Stripe SDK
   * @return the record stored in DynamoDB
   */
  public static org.seattleoba.data.model.stripe.Customer convert(
      final com.stripe.model.Customer input) {
    return new org.seattleoba.data.model.stripe.Customer(
        input.getId(), input.getEmail(), input.getName());
  }
}
