package io.nats.client.impl;

import io.nats.client.MessageHandler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;

import java.time.Duration;
import java.util.function.Predicate;

public class VertxDispatcherMessageQueue extends MessageQueue {
    private final VertxDispatcher dispatcher;
    private final Vertx vertx;

    VertxDispatcherMessageQueue(VertxDispatcher dispatcher, Vertx vertx) {
        super(true);
        this.dispatcher = dispatcher;
        this.vertx = vertx;
    }

    @Override
    void pause() {
        running.set(STOPPED);
    }

    @Override
    void drain() {
        running.set(DRAINING);
    }

    @Override
    boolean push(NatsMessage msg) {
        ((ContextInternal)vertx.getOrCreateContext()).runOnContext(e -> {
            NatsSubscription sub = msg.getNatsSubscription();
            if (sub != null && sub.isActive()) {
                MessageHandler handler = dispatcher.subscriptionHandlers.get(sub.getSID());
                if (handler == null) {
                    handler = dispatcher.defaultHandler;
                }

                if (handler != null) {
                    sub.incrementDeliveredCount();
                    dispatcher.incrementDeliveredCount();

                    try {
                        handler.onMessage(msg);
                    }
                    catch (Exception ex) {
                        dispatcher.connection.processException(ex);
                    }

                    if (sub.reachedUnsubLimit()) {
                        dispatcher.connection.invalidate(sub);
                    }
                }
            }
        });
        return true;
    }

    @Override
    boolean push(NatsMessage msg, boolean internal) {
        throw new IllegalStateException("push(NatsMessage, boolean) not used.");
    }

    @Override
    void poisonTheQueue() {
        throw new IllegalStateException("poisonTheQueue not used.");
    }

    @Override
    boolean offer(NatsMessage msg) {
        throw new IllegalStateException("offer not used.");
    }

    @Override
    NatsMessage poll(Duration timeout) throws InterruptedException {
        return super.poll(timeout);
    }

    @Override
    NatsMessage pop(Duration timeout) throws InterruptedException {
        throw new IllegalStateException("offer not used.");
    }

    @Override
    NatsMessage accumulate(long maxSize, long maxMessages, Duration timeout) throws InterruptedException {
        throw new IllegalStateException("accumulate not used.");
    }

    @Override
    NatsMessage popNow() throws InterruptedException {
        throw new IllegalStateException("popNow not used.");
    }

    @Override
    long length() {
        return 0;
    }

    @Override
    long sizeInBytes() {
        return 0;
    }

    @Override
    void filter(Predicate<NatsMessage> p) {
        throw new IllegalStateException("filter not used.");
    }
}
