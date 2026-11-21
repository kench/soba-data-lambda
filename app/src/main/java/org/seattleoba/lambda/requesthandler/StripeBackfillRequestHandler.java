package org.seattleoba.lambda.requesthandler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.stripe.StripeClient;
import com.stripe.exception.StripeException;
import com.stripe.model.HasId;
import com.stripe.model.StripeCollection;
import com.stripe.param.BalanceTransactionListParams;
import com.stripe.param.ChargeListParams;
import com.stripe.param.CustomerListParams;
import com.stripe.param.PaymentIntentListParams;
import com.stripe.param.RefundListParams;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import javax.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.seattleoba.data.persistence.stripe.StripeBalanceTransactionStore;
import org.seattleoba.data.persistence.stripe.StripeChargeStore;
import org.seattleoba.data.persistence.stripe.StripeCustomerStore;
import org.seattleoba.data.persistence.stripe.StripePaymentIntentStore;
import org.seattleoba.data.persistence.stripe.StripeRefundStore;
import org.seattleoba.lambda.converter.stripe.BalanceTransactionConverter;
import org.seattleoba.lambda.converter.stripe.ChargeConverter;
import org.seattleoba.lambda.converter.stripe.CustomerConverter;
import org.seattleoba.lambda.converter.stripe.PaymentIntentConverter;
import org.seattleoba.lambda.converter.stripe.RefundConverter;
import org.seattleoba.lambda.model.StripeBackfillRequest;
import org.seattleoba.lambda.model.StripeBackfillResult;

/**
 * Pages through the Stripe list APIs and writes every object it finds to its DynamoDB table.
 *
 * <p>Objects are stored straight from the list responses, using the same converters and stores as
 * {@link StripeDataSyncRequestHandler}, so a backfilled record matches one written by the event
 * processor. Writes are idempotent, so a backfill can be re-run safely.
 *
 * <p>Before each page the handler checks how much Lambda time is left. When it runs low, or when
 * Stripe returns an error, the handler stops and returns a {@link
 * StripeBackfillResult#nextRequest()} that resumes from the last object stored.
 */
public class StripeBackfillRequestHandler
    implements RequestHandler<StripeBackfillRequest, StripeBackfillResult> {
  private static final Logger LOG = LogManager.getLogger(StripeBackfillRequestHandler.class);

  static final String BALANCE_TRANSACTION_RESOURCE_TYPE = "balance-transaction";
  static final String CHARGE_RESOURCE_TYPE = "charge";
  static final String CUSTOMER_RESOURCE_TYPE = "customer";
  static final String PAYMENT_INTENT_RESOURCE_TYPE = "payment-intent";
  static final String REFUND_RESOURCE_TYPE = "refund";

  /** Resource types backfilled when the request names none, in the order they run. */
  static final List<String> ALL_RESOURCE_TYPES =
      List.of(
          CUSTOMER_RESOURCE_TYPE,
          PAYMENT_INTENT_RESOURCE_TYPE,
          CHARGE_RESOURCE_TYPE,
          REFUND_RESOURCE_TYPE,
          BALANCE_TRANSACTION_RESOURCE_TYPE);

  /** Largest page size the Stripe list APIs allow. */
  static final long PAGE_SIZE = 100L;

  /** Stop fetching new pages once less than this much Lambda time remains. */
  static final long MIN_REMAINING_MILLIS = 60_000L;

  private final StripeClient stripeClient;
  private final StripeBalanceTransactionStore stripeBalanceTransactionStore;
  private final StripeChargeStore stripeChargeStore;
  private final StripeCustomerStore stripeCustomerStore;
  private final StripePaymentIntentStore stripePaymentIntentStore;
  private final StripeRefundStore stripeRefundStore;

  /**
   * Creates the handler.
   *
   * @param stripeClient Stripe API client
   * @param stripeBalanceTransactionStore store for balance transactions
   * @param stripeChargeStore store for charges
   * @param stripeCustomerStore store for customers
   * @param stripePaymentIntentStore store for payment intents
   * @param stripeRefundStore store for refunds
   */
  @Inject
  public StripeBackfillRequestHandler(
      final StripeClient stripeClient,
      final StripeBalanceTransactionStore stripeBalanceTransactionStore,
      final StripeChargeStore stripeChargeStore,
      final StripeCustomerStore stripeCustomerStore,
      final StripePaymentIntentStore stripePaymentIntentStore,
      final StripeRefundStore stripeRefundStore) {
    this.stripeClient = stripeClient;
    this.stripeBalanceTransactionStore = stripeBalanceTransactionStore;
    this.stripeChargeStore = stripeChargeStore;
    this.stripeCustomerStore = stripeCustomerStore;
    this.stripePaymentIntentStore = stripePaymentIntentStore;
    this.stripeRefundStore = stripeRefundStore;
  }

  /**
   * Runs the backfill.
   *
   * @param input backfill request; null is treated as an empty request
   * @param context Lambda context, used to stop before the function times out; may be null
   * @return counts of stored and failed objects, and a request that resumes the run if it stopped
   * @throws IllegalArgumentException if the request names an unsupported resource type
   */
  @Override
  public StripeBackfillResult handleRequest(
      final StripeBackfillRequest input, final Context context) {
    final StripeBackfillRequest request =
        input == null ? new StripeBackfillRequest(null, null, null, null) : input;
    final List<String> resourceTypes =
        request.resourceTypes() == null || request.resourceTypes().isEmpty()
            ? ALL_RESOURCE_TYPES
            : List.copyOf(request.resourceTypes());
    for (final String resourceType : resourceTypes) {
      if (!ALL_RESOURCE_TYPES.contains(resourceType)) {
        throw new IllegalArgumentException(String.format("Unknown resource type %s", resourceType));
      }
    }

    final Map<String, Integer> stored = new LinkedHashMap<>();
    final Progress progress = new Progress();
    String startingAfter = request.startingAfter();
    for (int i = 0; i < resourceTypes.size(); i++) {
      final String resourceType = resourceTypes.get(i);
      progress.cursor = startingAfter;
      progress.stored = 0;
      startingAfter = null;

      final boolean finished =
          backfill(resourceType, request.createdGte(), request.createdLte(), progress, context);
      stored.put(resourceType, progress.stored);
      if (!finished) {
        final StripeBackfillRequest nextRequest =
            new StripeBackfillRequest(
                List.copyOf(resourceTypes.subList(i, resourceTypes.size())),
                request.createdGte(),
                request.createdLte(),
                progress.cursor);
        LOG.info("Backfill stopped early; resume with {}", nextRequest);
        return new StripeBackfillResult(false, stored, progress.failed, nextRequest);
      }
      LOG.info("Backfilled {} {} objects", progress.stored, resourceType);
    }
    return new StripeBackfillResult(true, stored, progress.failed, null);
  }

  private boolean backfill(
      final String resourceType,
      final Long createdGte,
      final Long createdLte,
      final Progress progress,
      final Context context) {
    return switch (resourceType) {
      case BALANCE_TRANSACTION_RESOURCE_TYPE ->
          backfillPages(
              resourceType,
              cursor -> {
                final BalanceTransactionListParams.Builder params =
                    BalanceTransactionListParams.builder().setLimit(PAGE_SIZE);
                if (cursor != null) {
                  params.setStartingAfter(cursor);
                }
                if (createdGte != null || createdLte != null) {
                  params.setCreated(
                      BalanceTransactionListParams.Created.builder()
                          .setGte(createdGte)
                          .setLte(createdLte)
                          .build());
                }
                return stripeClient.v1().balanceTransactions().list(params.build());
              },
              balanceTransaction ->
                  stripeBalanceTransactionStore.updateBalanceTransaction(
                      BalanceTransactionConverter.convert(balanceTransaction)),
              progress,
              context);
      case CHARGE_RESOURCE_TYPE ->
          backfillPages(
              resourceType,
              cursor -> {
                final ChargeListParams.Builder params =
                    ChargeListParams.builder().setLimit(PAGE_SIZE);
                if (cursor != null) {
                  params.setStartingAfter(cursor);
                }
                if (createdGte != null || createdLte != null) {
                  params.setCreated(
                      ChargeListParams.Created.builder()
                          .setGte(createdGte)
                          .setLte(createdLte)
                          .build());
                }
                return stripeClient.v1().charges().list(params.build());
              },
              charge -> stripeChargeStore.updateCharge(ChargeConverter.convert(charge)),
              progress,
              context);
      case CUSTOMER_RESOURCE_TYPE ->
          backfillPages(
              resourceType,
              cursor -> {
                final CustomerListParams.Builder params =
                    CustomerListParams.builder().setLimit(PAGE_SIZE);
                if (cursor != null) {
                  params.setStartingAfter(cursor);
                }
                if (createdGte != null || createdLte != null) {
                  params.setCreated(
                      CustomerListParams.Created.builder()
                          .setGte(createdGte)
                          .setLte(createdLte)
                          .build());
                }
                return stripeClient.v1().customers().list(params.build());
              },
              customer -> stripeCustomerStore.updateCustomer(CustomerConverter.convert(customer)),
              progress,
              context);
      case PAYMENT_INTENT_RESOURCE_TYPE ->
          backfillPages(
              resourceType,
              cursor -> {
                final PaymentIntentListParams.Builder params =
                    PaymentIntentListParams.builder().setLimit(PAGE_SIZE);
                if (cursor != null) {
                  params.setStartingAfter(cursor);
                }
                if (createdGte != null || createdLte != null) {
                  params.setCreated(
                      PaymentIntentListParams.Created.builder()
                          .setGte(createdGte)
                          .setLte(createdLte)
                          .build());
                }
                return stripeClient.v1().paymentIntents().list(params.build());
              },
              paymentIntent ->
                  stripePaymentIntentStore.updatePaymentIntent(
                      PaymentIntentConverter.convert(paymentIntent)),
              progress,
              context);
      default ->
          backfillPages(
              resourceType,
              cursor -> {
                final RefundListParams.Builder params =
                    RefundListParams.builder().setLimit(PAGE_SIZE);
                if (cursor != null) {
                  params.setStartingAfter(cursor);
                }
                if (createdGte != null || createdLte != null) {
                  params.setCreated(
                      RefundListParams.Created.builder()
                          .setGte(createdGte)
                          .setLte(createdLte)
                          .build());
                }
                return stripeClient.v1().refunds().list(params.build());
              },
              refund -> stripeRefundStore.updateRefund(RefundConverter.convert(refund)),
              progress,
              context);
    };
  }

  /**
   * Fetches pages until Stripe has no more, storing each object.
   *
   * @return true if every page was processed, false if the run stopped early
   */
  private <T extends HasId> boolean backfillPages(
      final String resourceType,
      final PageFetcher<T> pageFetcher,
      final Consumer<T> store,
      final Progress progress,
      final Context context) {
    while (true) {
      if (context != null && context.getRemainingTimeInMillis() < MIN_REMAINING_MILLIS) {
        LOG.warn("Running out of time while backfilling {}", resourceType);
        return false;
      }

      final StripeCollection<T> page;
      try {
        page = pageFetcher.fetch(progress.cursor);
      } catch (final StripeException exception) {
        LOG.error(
            "Unable to list {} objects after {} from Stripe",
            resourceType,
            progress.cursor,
            exception);
        return false;
      }

      final List<T> data = page.getData() == null ? List.of() : page.getData();
      for (final T object : data) {
        try {
          store.accept(object);
          progress.stored++;
        } catch (final RuntimeException exception) {
          LOG.error("Unable to store {} {}", resourceType, object.getId(), exception);
          progress.failed++;
        }
        progress.cursor = object.getId();
      }

      if (data.isEmpty() || !Boolean.TRUE.equals(page.getHasMore())) {
        return true;
      }
    }
  }

  /** Fetches one page of a Stripe list API. */
  @FunctionalInterface
  private interface PageFetcher<T extends HasId> {
    StripeCollection<T> fetch(String startingAfter) throws StripeException;
  }

  /** Mutable counters shared across the pages of one run. */
  private static final class Progress {
    private String cursor;
    private int stored;
    private int failed;
  }
}
