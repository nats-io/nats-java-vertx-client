package io.nats.vertx;


import io.nats.client.*;
import io.nats.client.impl.Headers;
import io.nats.vertx.impl.NatsClientImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
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
        return new NatsClientImpl(natsOptions.getNatsBuilder(), natsOptions);
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
     * Get interface to Key Value.
     * @param bucketName the bucket name
     * @return Key Value instance
     */
    Future<NatsKeyValue> keyValue(String bucketName);

    /**
     * Get interface to Key Value.
     * @param bucketName the bucket name
     * @param options KeyValue options.
     * @return Key Value instance
     */
    Future<NatsKeyValue> keyValue(String bucketName, KeyValueOptions options);

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
     * Send a request. The returned future will be completed when the
     * response comes back.
     *
     * @param subject the subject for the service that will handle the request
     * @param headers Optional headers to publish with the message.
     * @param body the content of the message
     * @return a Future for the response, which may be cancelled on error or timed out
     */
    Future<Message> request(String subject, Headers headers, byte[] body);

    /**
     * Send a request. The returned future will be completed when the
     * response comes back.
     *
     * @param subject the subject for the service that will handle the request
     * @param body the content of the message
     * @param headers Optional headers to publish with the message.
     * @param timeout the time to wait for a response
     * @return a Future for the response, which may be cancelled on error or timed out
     */
    Future<Message> requestWithTimeout(String subject, Headers headers, byte[] body, Duration timeout);

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
     * Send a message to the specified subject. The message body <strong>will
     * not</strong> be copied. The expected usage with string content is something
     * like:
     *
     * <pre>
     *
     * Headers h = new Headers().put("key", "value");
     * var future = nc.publish("destination", h, "message".getBytes("UTF-8"))
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     *
     * See {@link #publish(String, String, byte[]) publish()} for more details on
     * publish during reconnect.
     *
     * @param subject the subject to send the message to
     * @param headers Optional headers to publish with the message.
     * @param body the message body
     * @throws IllegalStateException if the reconnect buffer is exceeded
     */
    Future<Void> publish(String subject, Headers headers, byte[] body);

    /**
     * Send a request to the specified subject, providing a replyTo subject. The
     * message body <strong>will not</strong> be copied. The expected usage with
     * string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * Headers h = new Headers().put("key", "value");
     * var future = nc.publish("destination", "reply-to", h, "message".getBytes("UTF-8"))
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * <p>
     * During reconnect the client will try to buffer messages. The buffer size is set
     * in the connect options, see {@link Options.Builder#reconnectBufferSize(long) reconnectBufferSize()}
     * with a default value of {@link Options#DEFAULT_RECONNECT_BUF_SIZE 8 * 1024 * 1024} bytes.
     * If the buffer is exceeded an IllegalStateException is thrown. Applications should use
     * this exception as a signal to wait for reconnect before continuing.
     * </p>
     * @param subject the subject to send the message to
     * @param replyTo the subject the receiver should send the response to
     * @param headers Optional headers to publish with the message.
     * @param body the message body
     * @throws IllegalStateException if the reconnect buffer is exceeded
     */
    Future<Void> publish(String subject, String replyTo, Headers headers, byte[] body);

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

    Future<Void> close();

    @Override
    default Future<Void> end() {
        return close();
    }
}
