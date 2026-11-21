package org.seattleoba.lambda.converter.stripe;

/** Converts Stripe SDK balance transactions to the stored {@code soba-dataformats} record. */
public class BalanceTransactionConverter {
  /**
   * Copies the stored fields of a Stripe balance transaction.
   *
   * @param input balance transaction returned by the Stripe SDK
   * @return the record stored in DynamoDB
   */
  public static org.seattleoba.data.model.stripe.BalanceTransaction convert(
      final com.stripe.model.BalanceTransaction input) {
    return new org.seattleoba.data.model.stripe.BalanceTransaction(
        input.getId(),
        input.getAmount(),
        input.getAvailableOn(),
        input.getCreated(),
        input.getCurrency(),
        input.getDescription(),
        input.getFee(),
        input.getNet(),
        input.getSource(),
        input.getStatus(),
        input.getType());
  }
}
