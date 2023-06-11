package io.nats.vertx;

import io.nats.client.Message;
import io.vertx.core.Future;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;

public interface SubscriptionReadStream {

    /**
     * Retrieve a message from the subscription.
     * @param batchSize batchSize The batch size, only use if you passed the right publish options.
     * @param maxWaitMillis the maximum time to wait for the first message, in milliseconds
     * @return future message.
     */
    Future<List<Message>> fetch(final int batchSize, final long maxWaitMillis);

    /**
     * Fetch a list of messages up to the batch size, waiting no longer than maxWait.
     * This uses pullExpiresIn under the covers, and manages all responses from sub.nextMessage(...)
     * to only return regular JetStream messages. This can only be used when the subscription
     * is pull based. ! Pull subscriptions only. Push subscription will throw IllegalStateException
     * @param batchSize batchSize The batch size, only use if you passed the right publish options.
     * @param maxWait the maximum time to wait for the first message, in milliseconds
     * @return future message.
     */
    default Future<List<Message>> fetch( final int batchSize, final Duration maxWait) {
        return fetch(batchSize, maxWait.toMillis());
    }

    /**
     * Prepares an iterator. This uses pullExpiresIn under the covers, and manages all responses.
     * The iterator will have no messages if it does not receive the first message within
     * the max wait period. It will stop if the batch is fulfilled or if there are fewer
     * than batch size messages. 408 Status messages are ignored and will not count toward the
     * fulfilled batch size. ! Pull subscriptions only. Push subscription will throw IllegalStateException
     * @param subject subject The subject for the subscription.
     * @param batchSize batchSize The batch size, only use if you passed the right publish options.
     * @param maxWaitMillis the maximum time to wait for the first message, in milliseconds
     * @return future message.
     */
    Future<Iterator<Message>> iterate(final String subject, final int batchSize, final long maxWaitMillis);

    /**
     * Prepares an iterator. This uses pullExpiresIn under the covers, and manages all responses.
     * The iterator will have no messages if it does not receive the first message within
     * the max wait period. It will stop if the batch is fulfilled or if there are fewer
     * than batch size messages. 408 Status messages are ignored and will not count toward the
     * fulfilled batch size. ! Pull subscriptions only. Push subscription will throw IllegalStateException
     * @param subject subject The subject for the subscription.
     * @param batchSize batchSize The batch size, only use if you passed the right publish options.
     * @param maxWait the maximum time to wait for the first message, in milliseconds
     * @return future message.
     */
    default Future<Iterator<Message>> iterate(final String subject, final int batchSize, final Duration maxWait) {
        return iterate(subject, batchSize, maxWait.toMillis());
    }

    /**
     * Unsubscribe this subscription and stop listening for messages.
     *
     * <p>Stops messages to the subscription locally and notifies the server.
     *
     * @throws IllegalStateException if the subscription belongs to a dispatcher, or is not active
     */
    Future<Void> unsubscribeAsync();
}
