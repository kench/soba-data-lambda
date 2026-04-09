package org.seattleoba.lambda.converter.stripe;

public class CustomerConverter {
    public static org.seattleoba.data.model.stripe.Customer convert(
            final com.stripe.model.Customer input) {
        return new org.seattleoba.data.model.stripe.Customer(
                input.getId(),
                input.getEmail(),
                input.getName());
    }
}
