package org.seattleoba.lambda.converter.stripe;

public final class PaymentIntentConverter {
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
