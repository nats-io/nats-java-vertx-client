package io.nats.vertx.impl;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import io.nats.vertx.NatsStream;
import io.nats.vertx.NatsVertxMessage;
import io.nats.vertx.SubscriptionReadStream;
import io.vertx.core.*;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.streams.WriteStream;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * NATS stream implementation.
 */
public class NatsStreamImpl implements NatsStream {

    private final JetStream jetStream;
    private final Vertx vertx;

    private final ConcurrentHashMap<String, Dispatcher> dispatcherMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, JetStreamSubscription> subscriptionMap = new ConcurrentHashMap<>();


    private final Connection connection;

    private final AtomicReference<Handler<Throwable>> exceptionHandler = new AtomicReference<>();
    /**
     * Create instance
     * @param jetStream jetStream
     * @param connection Nats connection
     * @param vertx vertx
     */
    public NatsStreamImpl(final JetStream jetStream, final Connection connection,  final Vertx vertx,
                          final Handler<Throwable> exceptionHandler) {

        this.connection = connection;
        this.jetStream = jetStream;
        this.vertx = vertx;
        this.exceptionHandler.set(exceptionHandler);
    }

    private ContextInternal context() {
        return (ContextInternal)  vertx.getOrCreateContext();
    }

    @Override
    public WriteStream<Message> exceptionHandler(Handler<Throwable> handler) {
        exceptionHandler.set(handler);
        return this;
    }

    @Override
    public Future<Void> write(Message data) {
        final Promise<Void> promise = context().promise();
        doPublish(data, promise);
        return promise.future();
    }

    @Override
    public void write(Message data, Handler<AsyncResult<Void>> handler) {
        final Promise<Void> promise = context().promise();
        doPublish(data, promise);
        handler.handle(promise.future());

    }

    private void doPublish(Message data, Promise<Void> promise) {
        try {
            PublishAck publish = jetStream.publish(data);
            if (publish.isDuplicate()) {
                promise.fail("Duplicate message " + publish);
            } else if (publish.hasError()) {
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
        final Promise<Void> promise = context().promise();
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
        final Promise<PublishAck> promise = context().promise();
        context().executeBlocking(event -> {
            try {
                PublishAck ack = jetStream.publish(data);
                promise.complete(ack);
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    @Override
    public Future<PublishAck> publish(String subject, String message) {
        return this.publish(subject, message.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Future<PublishAck> publish(final String subject, final byte[] message) {
        final Promise<PublishAck> promise = context().promise();

        context().executeBlocking(event -> {
            try {
                final PublishAck ack = jetStream.publish(subject, message);
                promise.complete(ack);
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);

        return promise.future();
    }

    @Override
    public void publish(Message data, Handler<AsyncResult<PublishAck>> handler) {
        final Promise<PublishAck> promise = context().promise();
        context().executeBlocking(event -> {
            try {
                PublishAck ack = jetStream.publish(data);
                promise.complete(ack);
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        handler.handle(promise.future());
    }

    @Override
    public Future<PublishAck> publish(Message data, PublishOptions options) {
        final Promise<PublishAck> promise = context().promise();
        context().executeBlocking(event -> {
            try {
                final PublishAck ack = jetStream.publish(data, options);
                promise.complete(ack);
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    @Override
    public Future<PublishAck> publish(String subject, Headers headers, byte[] body) {
        final Promise<PublishAck> promise = context().promise();
        context().executeBlocking(event -> {
            try {
                final PublishAck ack = jetStream.publish(subject, headers, body);
                promise.complete(ack);
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    @Override
    public Future<PublishAck> publish(String subject, Headers headers, byte[] body, PublishOptions options) {
        final Promise<PublishAck> promise = context().promise();
        context().executeBlocking(event -> {
            try {
                final PublishAck ack = jetStream.publish(subject, headers, body, options);
                promise.complete(ack);
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    @Override
    public Future<Void> subscribe(String subject, Handler<NatsVertxMessage> handler, boolean autoAck, PushSubscribeOptions so) {
        final Promise<Void> promise = context().promise();
        final Handler<Message> handlerWrapper = event -> handler.handle(new NatsVertxMessageImpl(event, context()));
        final Dispatcher dispatcher = connection.createDispatcher();
        context().executeBlocking(event -> {
            try {
                jetStream.subscribe(subject, dispatcher, handlerWrapper::handle, autoAck, so);
                dispatcherMap.put(subject, dispatcher);
                promise.complete();
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    @Override
    public Future<Void> subscribe(String subject, String queue, final Handler<NatsVertxMessage> handler, boolean autoAck, PushSubscribeOptions so) {
        final Promise<Void> promise = context().promise();
        final Handler<Message> handlerWrapper = event -> handler.handle(new NatsVertxMessageImpl(event, context()));
        final Dispatcher dispatcher = connection.createDispatcher();
        context().executeBlocking(event -> {
            try {
                jetStream.subscribe(subject, queue, dispatcher, handlerWrapper::handle, autoAck, so);
                dispatcherMap.put(subject, dispatcher);
                promise.complete();
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }


    @Override
    public Future<SubscriptionReadStream> subscribe(final String subject, final PullSubscribeOptions so) {
        final Promise<SubscriptionReadStream> promise = context().promise();
        context().executeBlocking(evt -> {
            try {
                final JetStreamSubscription subscription = so != null ? jetStream.subscribe(subject, so) : jetStream.subscribe(subject);

                final SubscriptionReadStream subscriptionReadStream = new SubscriptionReadStreamImpl(context(), subscription, exceptionHandler);
                subscriptionMap.put(subject, subscription);
                promise.complete(subscriptionReadStream);
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    @Override
    public Future<SubscriptionReadStream> subscribe(String subject) {
       return subscribe(subject, null);
    }

    public Future<List<NatsVertxMessage>> fetch(final String subject, final int batchSize, final long maxWaitMillis) {
        final Promise<List<NatsVertxMessage>> promise = context().promise();
        context().executeBlocking(evt -> {
            try {
                final JetStreamSubscription jetStreamSubscription = subscriptionMap.get(subject);
                if (jetStreamSubscription == null) {
                    throw new IllegalStateException("Subscription not found " + subject);
                }
                final List<Message> messages = jetStreamSubscription.fetch(batchSize, maxWaitMillis);
                promise.complete(NatsVertxMessageImpl.listOf(messages, context()));
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    @Override
    public Future<Iterator<NatsVertxMessage>> iterate(String subject, int batchSize, long maxWaitMillis) {
        final Promise<Iterator<NatsVertxMessage>> promise = context().promise();
        context().executeBlocking(evt -> {
            try {
                final JetStreamSubscription jetStreamSubscription = subscriptionMap.get(subject);
                if (jetStreamSubscription == null) {
                    throw new IllegalStateException("Subscription not found " + subject);
                }
                final Iterator<Message> messages = jetStreamSubscription.iterate(batchSize, maxWaitMillis);
                promise.complete(new Iterator<NatsVertxMessage>() {
                    @Override
                    public boolean hasNext() {
                        return messages.hasNext();
                    }

                    @Override
                    public NatsVertxMessage next() {
                        return new NatsVertxMessageImpl(messages.next(), context());
                    }
                });
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }


    @Override
    public Future<Void> unsubscribe(final String subject) {
        final Promise<Void> promise = context().promise();
        context().executeBlocking(event -> {
            try {
                final Dispatcher dispatcher = dispatcherMap.get(subject);
                if (dispatcher == null) {

                    final JetStreamSubscription subscription = subscriptionMap.get(subject);
                    if (subscription == null) {
                        promise.fail("Subscription not found for unsubscribe op: " + subject);
                    } else {
                        subscription.unsubscribe();
                        subscriptionMap.remove(subject);
                    }
                } else {
                    dispatcherMap.remove(subject);
                    connection.closeDispatcher(dispatcher);
                    promise.complete();
                }
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }


    private void handleException(Promise<?> promise, Exception e) {
        promise.fail(e);
        exceptionHandler.get().handle(e);
    }
}
