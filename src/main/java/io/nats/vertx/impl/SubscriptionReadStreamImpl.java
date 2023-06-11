package io.nats.vertx.impl;

import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.vertx.SubscriptionReadStream;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.impl.ContextInternal;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class SubscriptionReadStreamImpl implements SubscriptionReadStream {
    private final ContextInternal context;
    private final JetStreamSubscription subscription;
    private final AtomicReference<Handler<Throwable>> exceptionHandler;

    public SubscriptionReadStreamImpl(ContextInternal context, JetStreamSubscription subscription, AtomicReference<Handler<Throwable>> exceptionHandler) {
        this.context = context;
        this.subscription = subscription;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public Future<List<Message>> fetch(int batchSize, long maxWaitMillis) {
        final Promise<List<Message>> promise = context.promise();
        context.executeBlocking(evt -> {
            try {
                final List<Message> messages = subscription.fetch(batchSize, maxWaitMillis);
                promise.complete(messages);
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    @Override
    public Future<Iterator<Message>> iterate(String subject, int batchSize, long maxWaitMillis) {
        final Promise<Iterator<Message>> promise = context.promise();
        context.executeBlocking(evt -> {
            try {
                final Iterator<Message> messages = subscription.iterate(batchSize, maxWaitMillis);
                promise.complete(messages);
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    @Override
    public Future<Void> unsubscribeAsync() {
        return null;
    }

    private void handleException(Promise<?> promise, Exception e) {
        promise.fail(e);
        exceptionHandler.get().handle(e);
    }
}
