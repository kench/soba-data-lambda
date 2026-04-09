package org.seattleoba.lambda.converter.stripe;

public class ChargeConverter {
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
                input.getStatus()
        );
    }
}
