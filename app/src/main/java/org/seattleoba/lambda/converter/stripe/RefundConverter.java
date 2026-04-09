package org.seattleoba.lambda.converter.stripe;

public class RefundConverter {
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
