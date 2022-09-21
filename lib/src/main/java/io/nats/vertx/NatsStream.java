package io.nats.vertx;


import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;
import java.io.IOException;

public interface NatsStream extends WriteStream<Message> {

    static NatsStream create(
            Options options,
            Connection connection,
            Dispatcher dispatcher,
            JetStreamOptions jetStreamOptions)
            throws IOException {
        return new NatsStreamImpl(options, connection, dispatcher, jetStreamOptions);
    }

    @Override
    NatsStream drainHandler(Handler<Void> handler);

    Future<PublishAck> publish(Message data);

    Future<PublishAck> publish(String subject, String message);

    void publish(Message data, Handler<AsyncResult<PublishAck>> handler);

    Future<Void> subscribe(
            String subject, Handler<Message> handler, boolean autoAck, PushSubscribeOptions so);

    Future<Void> subscribe(
            String subject,
            String queue,
            Handler<Message> handler,
            boolean autoAck,
            PushSubscribeOptions so);

    Future<Void> unsubscribe() throws InterruptedException;

    Future<PublishAck> publish(Message data, PublishOptions options);
}
