package io.nats.vertx.impl;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.vertx.NatsStream;
import io.nats.vertx.NatsVertxMessage;
import io.vertx.core.*;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.streams.WriteStream;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
    public Future<Void> subscribe(String subject, Handler<NatsVertxMessage> handler, boolean autoAck, PushSubscribeOptions so) {
        final Promise<Void> promise = context().promise();

        final Handler<Message> handlerWrapper = event -> handler.handle(new NatsVertxMessage() {
            @Override
            public Message message() {
                return event;
            }

            @Override
            public Vertx vertx() {
                return vertx;
            }
        });
        context().executeBlocking(event -> {
            try {

                final Dispatcher dispatcher = connection.createDispatcher();
                final Subscription subscribe = jetStream.subscribe(subject, dispatcher, msg -> handlerWrapper.handle(msg), autoAck, so);
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
        final Handler<Message> handlerWrapper = event -> handler.handle(new NatsVertxMessage() {
            @Override
            public Message message() {
                return event;
            }
            @Override
            public Vertx vertx() {
                return vertx;
            }
        });

        context().executeBlocking(event -> {
            try {
                final Dispatcher dispatcher = connection.createDispatcher();
                jetStream.subscribe(subject, queue, dispatcher, msg -> handlerWrapper.handle(msg), autoAck, so);
                dispatcherMap.put(subject, dispatcher);
                promise.complete();
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }


    @Override
    public Future<Void> subscribe(final String subject, final Handler<NatsVertxMessage> handler, final PullSubscribeOptions so,
                                  final int batchSize) {
        final Promise<Void> promise = context().promise();

        context().executeBlocking(evt -> {
            try {

                final JetStreamSubscription subscription = so != null ? jetStream.subscribe(subject, so) : jetStream.subscribe(subject);
                subscriptionMap.put(subject, subscription);
                context().executeBlocking(event -> {
                    try {
                        int messageCount = 0;
                        while (true) {

                            if (messageCount % batchSize == 0) {
                                subscription.pull(batchSize);
                            }
                            messageCount++;

                            final Message message = subscription.nextMessage(Duration.ofMillis(10));


                            if (message != null) {
                                final NatsVertxMessage nvMessage = new NatsVertxMessage() {
                                    @Override
                                    public Message message() {
                                        return message;
                                    }

                                    @Override
                                    public Vertx vertx() {
                                        return vertx;
                                    }
                                };

                                handler.handle(nvMessage);
                            } else {
                                if (!subscriptionMap.containsKey(subject)) {
                                    break;
                                }
                            }
                        }
                    } catch (Exception e) {
                        promise.fail(e);
                        exceptionHandler.get().handle(e);
                    }
                }, false);
                promise.complete();
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }


    @Override
    public Future<Void> subscribe(final String subject, final Handler<NatsVertxMessage> handler) {
        final Promise<Void> promise = context().promise();
        context().executeBlocking(evt -> {
            try {
                final JetStreamSubscription subscription = jetStream.subscribe(subject);
                subscriptionMap.put(subject, subscription);
                context().executeBlocking(event -> {
                    try {
                        while (true) {
                            final Message message = subscription.nextMessage(Duration.ofMillis(10));
                            if (message != null) {
                                final NatsVertxMessage nvMessage = new NatsVertxMessage() {
                                    @Override
                                    public Message message() {
                                        return message;
                                    }

                                    @Override
                                    public Vertx vertx() {
                                        return vertx;
                                    }
                                };
                                handler.handle(nvMessage);
                            } else {
                                if (!subscriptionMap.containsKey(subject)) {
                                    break;
                                }
                            }
                        }
                    } catch (Exception e) {
                        promise.fail(e);
                        exceptionHandler.get().handle(e);
                    }
                }, false);
                promise.complete();
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    @Override
    public Future<Void> subscribeBatch(final String subject, final Handler<List<NatsVertxMessage>> handler,
                                       final int batchSize, final Duration batchDuration,
                                       final PullSubscribeOptions so) {
        final Promise<Void> promise = context().promise();

        context().executeBlocking(evt -> {
            try {

                final JetStreamSubscription subscription = jetStream.subscribe(subject, so);
                subscriptionMap.put(subject, subscription);


                context().executeBlocking(event -> {
                    try {
                        while (true) {


                            List<Message> messages = subscription.fetch(batchSize, batchDuration);


                            if (messages != null && !messages.isEmpty()) {
                                final List<NatsVertxMessage> natsVertxMessages = messages.stream().map(m -> new NatsVertxMessage() {

                                            @Override
                                            public Message message() {
                                                return m;
                                            }

                                            @Override
                                            public Vertx vertx() {
                                                return vertx;
                                            }
                                        }
                                ).collect(Collectors.toList());


                                handler.handle(natsVertxMessages);
                            } else {
                                if (!subscriptionMap.containsKey(subject)) {
                                    break;
                                }
                            }
                        }
                    } catch (Exception e) {
                        promise.fail(e);
                        exceptionHandler.get().handle(e);
                    }
                }, false);
                promise.complete();
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
