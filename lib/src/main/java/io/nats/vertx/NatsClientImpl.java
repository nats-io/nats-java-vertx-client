package io.nats.vertx;

import io.nats.client.Connection;
import io.nats.client.JetStreamOptions;
import io.nats.client.Message;
import io.nats.client.Options;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;

public class NatsClientImpl implements NatsClient {
    public NatsClientImpl(Options config) {
    }

    @Override
    public Future<Void> connect() {
        return null;
    }

    @Override
    public Future<NatsStream> jetStream() {
        return null;
    }

    @Override
    public Future<NatsStream> jetStream(JetStreamOptions options) {
        return null;
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
    public NatsClient drainHandler(Handler<Void> handler) {
        return null;
    }

    @Override
    public void publish(Message data, Handler<AsyncResult<Void>> handler) {

    }

    @Override
    public Future<Void> publish(Message data) {
        return null;
    }

    @Override
    public Future<Void> publish(String subject, String replyTo, String message) {
        return null;
    }

    @Override
    public Future<Void> publish(String subject, String message) {
        return null;
    }

    @Override
    public void request(Message data, Handler<AsyncResult<Message>> handler) {

    }

    @Override
    public Future<Message> request(Message data) {
        return null;
    }

    @Override
    public Future<Message> request(String subject, String replyTo, String message) {
        return null;
    }

    @Override
    public Future<Message> request(String subject, String message) {
        return null;
    }

    @Override
    public Future<Void> subscribe(String subject, Handler<Message> handler) {
        return null;
    }

    @Override
    public Future<Void> subscribe(String subject, String queue, Handler<Message> handler) {
        return null;
    }

    @Override
    public Connection getConnection() {
        return null;
    }
}
