package io.nats.vertx;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import io.vertx.core.streams.WriteStream;

public class NatsStreamImpl implements NatsStream {
    public NatsStreamImpl(Options options, Connection connection, Dispatcher dispatcher, JetStreamOptions jetStreamOptions) {
    }

    @Override
    public WriteStream<Message> exceptionHandler(Handler<Throwable> handler) {
        return null;
    }

    @Override
    public Future<Void> write(Message data) {
        return null;
    }

    @Override
    public void write(Message data, Handler<AsyncResult<Void>> handler) {

    }

    @Override
    public void end(Handler<AsyncResult<Void>> handler) {

    }

    @Override
    public WriteStream<Message> setWriteQueueMaxSize(int maxSize) {
        return null;
    }

    @Override
    public boolean writeQueueFull() {
        return false;
    }

    @Override
    public NatsStream drainHandler(Handler<Void> handler) {
        return null;
    }

    @Override
    public Future<PublishAck> publish(Message data) {
        return null;
    }

    @Override
    public Future<PublishAck> publish(String subject, String message) {
        return null;
    }

    @Override
    public void publish(Message data, Handler<AsyncResult<PublishAck>> handler) {

    }

    @Override
    public Future<Void> subscribe(String subject, Handler<Message> handler, boolean autoAck, PushSubscribeOptions so) {
        return null;
    }

    @Override
    public Future<Void> subscribe(String subject, String queue, Handler<Message> handler, boolean autoAck, PushSubscribeOptions so) {
        return null;
    }

    @Override
    public Future<Void> unsubscribe() throws InterruptedException {
        return null;
    }

    @Override
    public Future<PublishAck> publish(Message data, PublishOptions options) {
        return null;
    }
}
