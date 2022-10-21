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
import java.util.HashMap;
import java.util.Map;

/**
 * NATS stream implementation.
 */
public class NatsStreamImpl implements NatsStream {

    private final JetStream jetStream;
    private final ContextInternal context;
    private final Vertx vertx;

    private final Map<String, Dispatcher> subscriptionMap = new HashMap<>();

    private static final Duration noWait = Duration.ofNanos(1);

    private static final Duration drainWait = Duration.ofMillis(50);
    private final Connection connection;

    private Handler<Throwable> exceptionHandler = event -> {
    };

    /**
     * Create instance
     * @param jetStream jetStream
     * @param connection Nats connection
     * @param contextInternal vertx context
     * @param vertx vertx
     */
    public NatsStreamImpl(final JetStream jetStream, final Connection connection, final ContextInternal contextInternal, final Vertx vertx) {

        this.connection = connection;
        this.jetStream = jetStream;
        this.context = contextInternal;
        this.vertx = vertx;
    }

    @Override
    public WriteStream<Message> exceptionHandler(Handler<Throwable> handler) {
        vertx.executeBlocking(event -> this.exceptionHandler = handler);
        return this;
    }

    @Override
    public Future<Void> write(Message data) {
        final Promise<Void> promise = context.promise();
        doPublish(data, promise);
        return promise.future();
    }

    @Override
    public void write(Message data, Handler<AsyncResult<Void>> handler) {
        final Promise<Void> promise = context.promise();
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
        try {
            PublishAck ack = jetStream.publish(data);
            promise.complete(ack);
        } catch (Exception e) {
            handleException(promise, e);
        }
        return promise.future();
    }

    @Override
    public Future<PublishAck> publish(String subject, String message) {
        return this.publish(subject, message.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Future<PublishAck> publish(final String subject, final byte[] message) {
        final Promise<PublishAck> promise = context.promise();
        try {
            final PublishAck ack = jetStream.publish(subject, message);
            promise.complete(ack);
        } catch (Exception e) {
            handleException(promise, e);
        }
        return promise.future();
    }

    @Override
    public void publish(Message data, Handler<AsyncResult<PublishAck>> handler) {
        final Promise<PublishAck> promise = context.promise();
        try {
            PublishAck ack = jetStream.publish(data);
            promise.complete(ack);
        } catch (Exception e) {
            handleException(promise, e);
        }
        handler.handle(promise.future());
    }

    @Override
    public Future<Void> subscribe(String subject, Handler<NatsVertxMessage> handler, boolean autoAck, PushSubscribeOptions so) {
        final Promise<Void> promise = context.promise();

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
        vertx.runOnContext(event -> {
            try {

                final Dispatcher dispatcher = connection.createDispatcher();
                final Subscription subscribe = jetStream.subscribe(subject, dispatcher, msg -> handlerWrapper.handle(msg), autoAck, so);
                subscriptionMap.put(subject, dispatcher);
                promise.complete();
            } catch (Exception e) {
                promise.fail(e);
                exceptionHandler.handle(e);
            }
        });
        return promise.future();
    }

    @Override
    public Future<Void> subscribe(String subject, String queue, final Handler<NatsVertxMessage> handler, boolean autoAck, PushSubscribeOptions so) {
        final Promise<Void> promise = context.promise();
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

        vertx.runOnContext(event -> {
            try {

                final Dispatcher dispatcher = connection.createDispatcher();
                jetStream.subscribe(subject, queue, dispatcher, msg -> handlerWrapper.handle(msg), autoAck, so);
                subscriptionMap.put(subject, dispatcher);
                promise.complete();
            } catch (Exception e) {
                promise.fail(e);
                exceptionHandler.handle(e);
            }
        });
        return promise.future();
    }

    @Override
    public Future<Void> unsubscribe(final String subject) {
        final Promise<Void> promise = context.promise();
        vertx.runOnContext(event -> {
            try {
                final Dispatcher dispatcher = subscriptionMap.get(subject);
                if (dispatcher == null) {
                    promise.fail("Subscription not found for unsubscribe op: " + subject);
                } else {
                    connection.closeDispatcher(dispatcher);
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
        try {
            final PublishAck ack = jetStream.publish(data, options);
            promise.complete(ack);
        } catch (Exception e) {
            handleException(promise, e);
        }
        return promise.future();
    }

    private void handleException(Promise<?> promise, Exception e) {
        promise.fail(e);
        throw new RuntimeException(e);
    }
}
