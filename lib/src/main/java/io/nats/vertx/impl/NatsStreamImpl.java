package io.nats.vertx.impl;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.vertx.NatsStream;
import io.vertx.core.*;

import io.vertx.core.impl.ContextInternal;
import io.vertx.core.streams.WriteStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class NatsStreamImpl implements NatsStream {

    private final JetStream jetStream;
    private final ContextInternal context;
    private final Vertx vertx;

    private final Map<String, Subscription> subscriptionMap = new HashMap<>();

    private static final Duration noWait = Duration.ofNanos(1);

    private Handler<Throwable> exceptionHandler = event -> {};

    public NatsStreamImpl(final JetStream jetStream, final ContextInternal contextInternal, final Vertx vertx) {

        this.jetStream = jetStream;
        this.context = contextInternal;
        this.vertx = vertx;
    }

    @Override
    public WriteStream<Message> exceptionHandler(Handler<Throwable> handler) {
        vertx.runOnContext(event -> this.exceptionHandler = handler);
        return this;
    }

    @Override
    public Future<Void> write(Message data) {

        final Promise<Void> promise = context.promise();
        vertx.runOnContext(event -> doPublish(data, promise));
        return promise.future();
    }

    @Override
    public void write(Message data, Handler<AsyncResult<Void>> handler) {
        final Promise<Void> promise = context.promise();
        vertx.runOnContext(event -> {
            doPublish(data, promise);
            handler.handle(promise.future());
        });
    }

    private void doPublish(Message data, Promise<Void> promise) {
        try {
            PublishAck publish = jetStream.publish(data);
            if (publish.isDuplicate()) {
                promise.fail("Duplicate message " + publish);
            } else if(publish.hasError()) {
                promise.fail(publish.getError() + " " + publish);
            } else {
                promise.complete();
            }
        } catch (Exception e) {
            handleException(promise, e);
        }
    }

    @Override
    public void end(Handler<AsyncResult<Void>> handler) {
        //No Op
        final Promise<Void> promise = context.promise();
        handler.handle(promise.future());
    }

    @Override
    public WriteStream<Message> setWriteQueueMaxSize(int maxSize) {
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return false;
    }

    @Override
    public NatsStream drainHandler(Handler<Void> handler) {
        return this;
    }

    @Override
    public Future<PublishAck> publish(final Message data) {
        final Promise<PublishAck> promise = context.promise();
        vertx.runOnContext(event -> {

            try {
                PublishAck ack = jetStream.publish(data);
                promise.complete(ack);
            } catch (Exception e) {
                handleException(promise, e);
            }
        });
        return promise.future();
    }

    @Override
    public Future<PublishAck> publish(String subject, String message) {
        return this.publish(subject, message.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Future<PublishAck> publish(final String subject, final byte[] message) {
        final Promise<PublishAck> promise = context.promise();
        vertx.runOnContext(event -> {
            try {
                final PublishAck ack = jetStream.publish(subject, message);
                promise.complete(ack);
            } catch (Exception e) {
                handleException(promise, e);
            }
        });
        return promise.future();
    }

    @Override
    public void publish(Message data, Handler<AsyncResult<PublishAck>> handler) {
        final Promise<PublishAck> promise = context.promise();
        vertx.runOnContext(event -> {
            try {
                PublishAck ack = jetStream.publish(data);
                promise.complete(ack);
            } catch (Exception e) {
                handleException(promise, e);
            }
            handler.handle(promise.future());
        });
    }

    @Override
    public Future<Void> subscribe(String subject, Handler<Message> handler, boolean autoAck, PushSubscribeOptions so) {
        final Promise<Void> promise = context.promise();
        vertx.runOnContext(event -> {
            try {
                final Subscription subscribe = jetStream.subscribe(subject, so);

                subscriptionMap.put(subject, subscribe);
                vertx.runOnContext(event1 -> drainSubscription(handler, subscribe, autoAck));
                promise.complete();
            } catch (Exception e) {
                promise.fail(e);
                exceptionHandler.handle(e);
            }
        });
        return promise.future();
    }

    private void drainSubscription(Handler<Message> handler, Subscription subscribe, boolean autoAck) {
        try {
            Message message = subscribe.nextMessage(noWait);
            int count = 0;
            while (message!=null) {
                try {
                    handler.handle(message);
                } catch (Exception ex) {
                    this.exceptionHandler.handle(ex);
                }
                if (autoAck) {
                    message.ack();
                }
                count++;
                if (count > 10) {
                    vertx.runOnContext(event -> drainSubscription(handler, subscribe, autoAck));
                    break;
                } else {
                    message = subscribe.nextMessage(noWait);
                }
            }

            if (message == null) {
                vertx.setTimer(100, event -> drainSubscription(handler, subscribe, autoAck));
            }
        } catch (Exception e) {
            exceptionHandler.handle(e);
        }
    }

    @Override
    public Future<Void> subscribe(String subject, String queue, Handler<Message> handler, boolean autoAck, PushSubscribeOptions so) {
        final Promise<Void> promise = context.promise();
        vertx.runOnContext(event -> {
            try {
                final Subscription subscribe = jetStream.subscribe(subject, queue, so);
                subscriptionMap.put(subject, subscribe);
                vertx.runOnContext(event1 -> drainSubscription(handler, subscribe, autoAck));
                promise.complete();
            } catch (Exception e) {
                handleException(promise, e);
            }
        });
        return promise.future();
    }

    @Override
    public Future<Void> unsubscribe(final String subject) {
        final Promise<Void> promise = context.promise();
        vertx.runOnContext(event -> {
            try {
                final Subscription subscription = subscriptionMap.get(subject);
                if (subscription == null) {
                    promise.fail("Subscription not found for unsubscribe op: " + subscription);
                } else {
                    subscription.unsubscribe();
                    promise.complete();
                }
            } catch (Exception e) {
                handleException(promise, e);
            }
        });
        return promise.future();
    }

    @Override
    public Future<PublishAck> publish(Message data, PublishOptions options) {
        final Promise<PublishAck> promise = context.promise();
        vertx.runOnContext(event -> {
            try {
                final PublishAck ack = jetStream.publish(data, options);
                promise.complete(ack);
            } catch (Exception e) {
                handleException(promise, e);
            }
        });
        return promise.future();
    }

    private void handleException(Promise<?> promise, Exception e) {
        promise.fail(e);
        throw new RuntimeException(e);
    }
}
