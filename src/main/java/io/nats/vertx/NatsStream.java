package io.nats.vertx;


import io.nats.client.*;
import io.nats.client.api.PublishAck;
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
            String subject, Handler<Message> handler, boolean autoAck, PushSubscribeOptions so);

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
            Handler<Message> handler,
            boolean autoAck,
            PushSubscribeOptions so);

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
}
