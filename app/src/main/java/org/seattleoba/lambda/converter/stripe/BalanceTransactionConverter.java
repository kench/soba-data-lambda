package org.seattleoba.lambda.converter.stripe;

public class BalanceTransactionConverter {
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
