package io.nats.vertx;


import io.nats.client.*;
import io.nats.vertx.impl.NatsClientImpl;
import io.vertx.core.*;
import io.vertx.core.streams.WriteStream;

import java.time.Duration;

/**
 * NATS client that implements Vert.x WriteStream.
 */
public interface NatsClient extends WriteStream<Message> {

    /**
     * Create a NatsClient.
     * @param natsOptions options to create NatsClient
     * @return NatsClient.
     */
    static NatsClient create(final NatsOptions natsOptions) {
        return new NatsClientImpl(natsOptions.getNatsBuilder(), natsOptions.getVertx());
    }

    /**
     * Connect to NATS.
     * @return future that gets triggered when NATS successfully connects.
     */
    Future<Void> connect();

    /**
     * Get interface to JetStream.
     * @return JetStream interface that implements Vert.x Write Stream.
     */
    Future<NatsStream> jetStream();

    /**
     * Get interface to JetStream.
     * @param options The options to create JetStream interface.
     * @return JetStream interface that implements Vert.x Write Stream.
     */
    Future<NatsStream> jetStream(JetStreamOptions options);

    /**
     * Drain handler.
     * @param handler the handler
     * @return NatsClient
     */
    @Override
    NatsClient drainHandler(Handler<Void> handler);

    /**
     * Publish message.
     * @param data the message
     * @param handler callback handler to know the results of the publish operation.
     */
    void publish(Message data, Handler<AsyncResult<Void>> handler);

    /**
     *
     * Publish message.
     * @param data the message
     * @return future to know results of the publish operation.
     */
    Future<Void> publish(Message data);

    /**
     *
     * Publish message.
     * @param subject The Subject of hte message.
     * @param replyTo The replyTo for this message.
     * @param message The message data.
     * @return future to know results of the publish operation.
     */
    Future<Void> publish(String subject, String replyTo, String message);

    /**
     *
     * Publish message.
     * @param subject The Subject of hte message.
     * @param replyTo The replyTo for this message.
     * @param message The message data.
     * @return future to know results of the publish operation.
     */
    Future<Void> publish(String subject, String replyTo, byte[] message);

    /**
     *
     * Publish message.
     * @param subject The Subject of hte message.
     * @param message The message data.
     * @return future to know results of the publish operation.
     */
    Future<Void> publish(String subject, String message);

    /**
     *
     * Publish message.
     * @param subject The Subject of hte message.
     * @param message The message data.
     * @return future to know results of the publish operation.
     */
    Future<Void> publish(String subject, byte[] message);

    /**
     *
     * Send request.
     * @param data the message
     * @param handler callback handler to know the results of the request operation.
     */
    void request(Message data, Handler<AsyncResult<Message>> handler);

    /**
     *
     * Send request.
     * @param data the message
     * @return future to know results of the request operation.
     */
    Future<Message> request(Message data);

    /**
     *
     * Send request.
     * @param subject The message subject.
     * @param message the message
     * @return future to know results of the request operation.
     */
    Future<Message> request(String subject, String message);

    /**
     *
     * Send request.
     * @param subject The message subject.
     * @param message the message
     * @return future to know results of the request operation.
     */
    Future<Message> request(String subject, byte[] message);


    /**
     *
     * Send request.
     * @param data the message
     * @param handler callback handler to know the results of the request operation.
     */
    void request(Message data, Handler<AsyncResult<Message>> handler, Duration timeout);

    /**
     *
     * Send request.
     * @param data the message
     * @return future to know results of the request operation.
     */
    Future<Message> request(Message data, Duration timeout);

    /**
     *
     * Send request.
     * @param subject The message subject.
     * @param message the message
     * @return future to know results of the request operation.
     */
    Future<Message> request(String subject, String message, Duration timeout);

    /**
     *
     * Send request.
     * @param subject The message subject.
     * @param message the message
     * @return future to know results of the request operation.
     */
    Future<Message> request(String subject, byte[] message, Duration timeout);

    /**
     *
     * Subscribe to subject.
     * @param subject The subscription subject.
     * @param handler Callback handler to know results of the request operation.
     * @return future to know results of the subscribe operation.
     */
    Future<Void> subscribe(String subject, Handler<Message> handler);

    /**
     *
     * Subscribe to subject.
     * @param subject The subscription subject.
     * @param queue The queue group listening to the subject.
     * @param handler Handler to get messages from the subscription.
     * @return future to know results of the subscribe operation.
     */
    Future<Void> subscribe(String subject, String queue, Handler<Message> handler);

    /**
     *
     * Unsubscribe to subject.
     * @param subject The subscription subject.
     * @return future to know results of the subscribe operation.
     */
    Future<Void> unsubscribe(String subject);

    /**
     * Get the underlying NATS connection
     * @return Low level NATS connection.
     */
    Connection getConnection();
}
