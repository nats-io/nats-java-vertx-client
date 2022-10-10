package io.nats.vertx;


import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;

public interface NatsStream extends WriteStream<Message> {

    @Override
    NatsStream drainHandler(Handler<Void> handler);

    Future<PublishAck> publish(Message data);

    Future<PublishAck> publish(String subject, String message);

    Future<PublishAck> publish(String subject, byte[] message);

    void publish(Message data, Handler<AsyncResult<PublishAck>> handler);

    Future<Void> subscribe(
            String subject, Handler<Message> handler, boolean autoAck, PushSubscribeOptions so);

    Future<Void> subscribe(
            String subject,
            String queue,
            Handler<Message> handler,
            boolean autoAck,
            PushSubscribeOptions so);

    Future<Void> unsubscribe(String subject);

    Future<PublishAck> publish(Message data, PublishOptions options);
}
