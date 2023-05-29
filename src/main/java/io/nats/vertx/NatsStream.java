package io.nats.vertx;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;

/**
 * Provides a Vert.x WriteStream interface with Futures and Promises.
 */
public interface NatsStream extends WriteStream<Message> {

    /**
     * Drain handler
     * @param handler the handler
     * @return this stream
     */
    @Override
    NatsStream drainHandler(Handler<Void> handler);

    /**
     * Publish message.
     * @param data The message data.
     * @return Future returning the results of the publish operation.
     */
    Future<PublishAck> publish(Message data);

    /**
     * Publish message.
     * @param subject The message data.
     * @param message The message data.
     * @return Future returning the results of the publish operation.
     */
    Future<PublishAck> publish(String subject, String message);

    /**
     * Publish message.
     * @param subject The message data.
     * @param message The message data.
     * @return Future returning the results of the publish operation.
     */
    Future<PublishAck> publish(String subject, byte[] message);

    /**
     * Publish message.
     * @param data The message data.
     * @param handler handler returning the results of the publish operation.
     */
    void publish(Message data, Handler<AsyncResult<PublishAck>> handler);

    /**
     * Subscribe to JetStream stream
     * @param subject The subject of the stream.
     * @param handler The message handler to listen to messages from the stream.
     * @param autoAck Specify if message handler should auto acknowledge.
     * @param so The PushSubscribeOptions
     * @return future that returns status of subscription.
     */
    Future<Void> subscribe(
            String subject, Handler<NatsVertxMessage> handler, boolean autoAck, PushSubscribeOptions so);

    /**
     * Subscribe to JetStream stream
     * @param subject The subject of the stream.
     * @param queue The queue name to share messages accross consumers with the same queue name.
     * @param handler The message handler to listen to messages from the stream.
     * @param autoAck Specify if message handler should auto acknowledge.
     * @param so The PushSubscribeOptions
     * @return future that returns status of subscription.
     */
    Future<Void> subscribe(
            String subject,
            String queue,
            Handler<NatsVertxMessage> handler,
            boolean autoAck,
            PushSubscribeOptions so);


    /**
     * Subscribe to JetStream stream
     * @param subject The subject of the stream.
     * @param so The PullSubscribeOptions
     * @return future that returns status of subscription.
     */
    Future<Void> subscribe(
            String subject, PullSubscribeOptions so) ;


    /**
     * Subscribe to JetStream stream
     * @param subject The subject of the stream.
     * @return future that returns status of subscription.
     */
    Future<Void> subscribe(
            String subject) ;


    /**
     * Unsubscribe from the Stream.
     * @param subject Subject to unsubscribe from.
     * @return future that returns status of unsubscribe.
     */
    Future<Void> unsubscribe(String subject);

    /**
     * Publish message.
     * @param data The message data.
     * @param options Publish options.
     * @return  future returning the results of the publish operation.
     */
    Future<PublishAck> publish(Message data, PublishOptions options);


    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The default publish options will be used.
     * The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * Headers h = new Headers().put("foo", "bar");
     * js.publish("destination", h, "message".getBytes("UTF-8"))
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     *
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     *
     * @param subject the subject to send the message to
     * @param headers Optional headers to publish with the message.
     * @param body the message body
     * @return The ack.
     */
    Future<PublishAck> publish(String subject, Headers headers, byte[] body);


    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * Headers h = new Headers().put("foo", "bar");
     * js.publish("destination", h, "message".getBytes("UTF-8"), publishOptions)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     *
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     *
     * @param subject the subject to send the message to
     * @param headers Optional headers to publish with the message.
     * @param body the message body
     * @param options publisher options
     * @return The ack.
     */
    Future<PublishAck> publish(String subject, Headers headers, byte[] body, PublishOptions options);


    /**
     * Retrieve a message from the subscription.
     * @param subject subject The subject for the subscription.
     * @param batchSize batchSize The batch size, only use if you passed the right publish options.
     * @return future message.
     */
    Future<Message> nextMessage(final String subject, final int batchSize);

    /**
     * Retrieve a message from the subscription.
     * @param subject subject The subject for the subscription.
     * @return future message.
     */
    default  Future<Message> nextMessage(final String subject) {
        return nextMessage(subject, 0);
    }


    /**
     * Request to pull a batch of message from the subscription.
     * You need to call nextMessage after this to get the messages.
     * @param subject subject The subject for the subscription.
     * @return future message.
     */
    Future<Void> pull(final String subject, final int batchSize);

}
