package io.nats.vertx.impl;

import io.nats.client.*;
import io.nats.vertx.NatsClient;
import io.nats.vertx.NatsStream;
import io.vertx.core.*;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.streams.WriteStream;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


/**
 * NATS Client implementation.
 */
public class NatsClientImpl implements NatsClient {
    private static final Duration noWait = Duration.ofNanos(1);
    private final Vertx vertx;
    private Connection connection;

    private final ContextInternal context;
    private final Options options;
    private Promise<Void> connectFuture;
    private Handler<Throwable> exceptionHandler = event -> {};

    /**
     * Create new client implementation.
     * @param config config
     * @param vertx vertx
     */
    public NatsClientImpl(final Options.Builder config, final Vertx vertx) {
        this.vertx = vertx;
        context = (ContextInternal) vertx.getOrCreateContext();
        this.options = wireConnectListener(config, context);
    }

    private Options wireConnectListener(final Options.Builder config, final ContextInternal context) {
        final Options build = config.build();

        final Promise<Void> promise = context.promise();

        if (build.getConnectionListener() == null) {
            config.connectionListener((conn, type) -> {
                if (type == ConnectionListener.Events.CONNECTED) {
                    promise.complete();
                }
            });
        } else {
            final ConnectionListener connectionListener = build.getConnectionListener();

            config.connectionListener((conn, type) -> {
                if (type == ConnectionListener.Events.CONNECTED) {
                    promise.complete();
                }
                connectionListener.connectionEvent(conn, type);
            });
        }

        this.connectFuture = promise;

        return config.build();

    }

    /**
     * Connect.
     * @return connection status future.
     */
    @Override
    public Future<Void> connect() {
        vertx.runOnContext(event -> {
            try {
                connection = Nats.connect(options);
            } catch (Exception e) {
                handleException(connectFuture, e);
            }
        });

        vertx.setPeriodic(1000, new Handler<Long>() {
            @Override
            public void handle(Long event) {
                try {
                    if (connection != null && connection.getStatus() == Connection.Status.CONNECTED) {
                        connection.flush(Duration.ofSeconds(1));
                    }
                } catch (Exception e) {
                  exceptionHandler.handle(e);
                }
            }
        });

        return this.connectFuture.future();
    }

    /**
     * Return new JetStream stream instance.
     * @return JetStream.
     */
    @Override
    public Future<NatsStream> jetStream() {
        final Promise<NatsStream> promise = context.promise();
        vertx.runOnContext(event -> {
            try {

                final JetStream jetStream = connection.jetStream();
                promise.complete(new NatsStreamImpl(jetStream, this.connection, context, vertx));
            } catch (Exception e) {
                handleException(promise, e);
            }

        });
        return promise.future();
    }

    /**
     * Return new JetStream stream instance.
     * @param options JetStream options.
     * @return JetStream.
     */
    @Override
    public Future<NatsStream> jetStream(final JetStreamOptions options) {
        final Promise<NatsStream> promise = context.promise();
        vertx.runOnContext(event -> {
            try {
                final JetStream jetStream = connection.jetStream(options);
                promise.complete(new NatsStreamImpl(jetStream, this.connection, context, vertx));
            } catch (Exception e) {
                handleException(promise, e);
            }
        });
        return promise.future();
    }

    /**
     * Wire the exception handler
     * @param handler  the exception handler
     * @return this stream.
     */
    @Override
    public WriteStream<Message> exceptionHandler(Handler<Throwable> handler) {
        vertx.runOnContext(event -> this.exceptionHandler = handler);
        return this;
    }

    /**
     * Write the message
     * @param data  the data to write
     * @return status of write operation future.
     */
    @Override
    public Future<Void> write(final Message data) {
        final Promise<Void> promise = context.promise();
        vertx.runOnContext(event -> {
            try {
                connection.publish(data);
                promise.complete();
            } catch (Exception e) {
                handleException(promise, e);
            }
        });
        return promise.future();
    }

    /**
     * Write the message
     * @param data  the data to write
     * @param handler status of write operation future.
     */
    @Override
    public void write(Message data, Handler<AsyncResult<Void>> handler) {
        final Promise<Void> promise = context.promise();
        vertx.runOnContext(event -> {
            try {
                connection.publish(data);
                promise.complete();
                handler.handle(promise.future());
            } catch (Exception e) {
                handleExceptionWithHandler(handler, promise, e);
            }
        });
    }


    /**
     * End and close this.
     * @param handler End Handler.
     */
    @Override
    public void end(Handler<AsyncResult<Void>> handler) {
        final Promise<Void> promise = context.promise();
        vertx.runOnContext(event -> {
            try {
                connection.close();
                promise.complete();
                handler.handle(promise.future());
            } catch (Exception e) {
                handleExceptionWithHandler(handler, promise, e);
            }
        });
    }

    /**
     * Set Queue Max size
     * @param maxSize  the max size of the write stream
     * @return this stream
     */
    @Override
    public WriteStream<Message> setWriteQueueMaxSize(int maxSize) {
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return false;
    }

    @Override
    public NatsClient drainHandler(Handler<Void> handler) {
        return this;
    }

    @Override
    public void publish(Message data, Handler<AsyncResult<Void>> handler) {
        this.write(data, handler);
    }

    @Override
    public Future<Void> publish(Message data) {
        return this.write(data);
    }

    @Override
    public Future<Void> publish(String subject, String replyTo, String message) {
        return this.publish(subject, replyTo, message.getBytes(StandardCharsets.UTF_8));
    }


    @Override
    public Future<Void> publish(String subject, String replyTo, byte[] message) {
        final Promise<Void> promise = context.promise();
        vertx.runOnContext(event -> {
            try {
                connection.publish(subject, replyTo, message);
                promise.complete();
            } catch (Exception e) {
                handleException(promise, e);
            }
        });
        return promise.future();
    }

    @Override
    public Future<Void> publish(String subject, String message) {
        return this.publish(subject,  message.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Future<Void> publish(String subject, byte[] message) {
        final Promise<Void> promise = context.promise();
        vertx.runOnContext(event -> {
            try {
                connection.publish(subject, message);
                promise.complete();
            } catch (Exception e) {
                handleException(promise, e);
            }
        });
        return promise.future();
    }

    private void handleException(Promise<?> promise, Exception e) {
        promise.fail(e);
        exceptionHandler.handle(e);
    }

    private void handleExceptionWithHandler(Handler<AsyncResult<Void>> handler,
                                            Promise<Void> promise, Exception e) {
        promise.fail(e);
        handler.handle(promise.future());
        exceptionHandler.handle(e);
    }

    @Override
    public void request(final Message data, final Handler<AsyncResult<Message>> handler) {
        final Promise<Message> promise = context.promise();
        vertx.executeBlocking((Handler<Promise<Void>>) event -> {
            try {
                final CompletableFuture<Message> request = connection.request(data);
                final Message message = request.get();
                promise.complete(message);
                handler.handle(promise.future());
            } catch (Exception e) {
                promise.fail(e);
                handler.handle(promise.future());
                exceptionHandler.handle(e);
            }
        });
    }

    @Override
    public Future<Message> request(Message data) {
        return vertx.executeBlocking(event -> {
            try {
                final CompletableFuture<Message> request = connection.request(data);
                final Message message = request.get();
                event.complete(message);
            } catch (Exception e) {
                handleException(event, e);
            }
        });
    }

    @Override
    public Future<Message> request(String subject, String message) {
        return this.request(subject, message.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Future<Message> request(String subject, byte[] message) {
        return vertx.executeBlocking(event -> {
            try {
                final CompletableFuture<Message> request = connection.request(subject, message);
                final Message result = request.get();
                event.complete(result);
            } catch (Exception e) {
                handleException(event, e);
            }
        });
    }

    @Override
    public void request(final Message data, final Handler<AsyncResult<Message>> handler, final Duration timeout) {
        final Promise<Message> promise = context.promise();
        vertx.executeBlocking((Handler<Promise<Void>>) event -> {
            try {
                final CompletableFuture<Message> request = connection.request(data);
                final Message message = request.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
                promise.complete(message);
                handler.handle(promise.future());
            } catch (Exception e) {
                promise.fail(e);
                handler.handle(promise.future());
                exceptionHandler.handle(e);
            }
        });
    }

    @Override
    public Future<Message> request(final Message data, final Duration timeout) {
        return vertx.executeBlocking(event -> {
            try {
                final CompletableFuture<Message> request = connection.request(data);
                final Message message = request.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
                event.complete(message);
            } catch (Exception e) {
                handleException(event, e);
            }
        });
    }

    @Override
    public Future<Message> request(String subject, String message, Duration timeout) {
        return  request(subject, message.getBytes(StandardCharsets.UTF_8), timeout);
    }

    @Override
    public Future<Message> request(final String subject, final byte[] message, final Duration timeout) {
        return vertx.executeBlocking(event -> {
            try {
                final CompletableFuture<Message> request = connection.request(subject, message);
                final Message result = request.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
                event.complete(result);
            } catch (Exception e) {
                handleException(event, e);
            }
        });
    }

    @Override
    public Future<Void> subscribe(String subject, Handler<Message> handler) {

        final Promise<Void> promise = context.promise();
        vertx.runOnContext(event -> {
            try {

                final Subscription subscribe = connection.subscribe(subject);
                vertx.executeBlocking(event1 -> drainSubscription(handler, subscribe));
                promise.complete();
            } catch (Exception e) {
                handleException(promise, e);
            }
        });
        return promise.future();
    }

    private void drainSubscription(Handler<Message> handler, Subscription subscribe) {
        try {
            Message message = subscribe.nextMessage(noWait);
            int count = 0;
            while (message!=null) {
                count++;
                try {
                    handler.handle(message);
                } catch (Exception e) {
                    exceptionHandler.handle(e);
                }
                message = subscribe.nextMessage(noWait);
            }
            vertx.setTimer(100, event -> vertx.executeBlocking(e -> drainSubscription(handler, subscribe)));
        } catch (Exception e) {
            exceptionHandler.handle(e);
        }
    }

    @Override
    public Future<Void> subscribe(String subject, String queue, Handler<Message> handler) {
        final Promise<Void> promise = context.promise();
        vertx.runOnContext(event -> {
            try {
                final Subscription subscribe = connection.subscribe(subject, queue);
                vertx.executeBlocking(event1 -> drainSubscription(handler, subscribe));
                promise.complete();
            } catch (Exception e) {
                handleException(promise, e);
            }
        });
        return promise.future();
    }

    @Override
    public Connection getConnection() {
        return this.connection;
    }
}
