package io.nats.vertx;


import io.nats.client.*;
import io.nats.vertx.impl.NatsClientImpl;
import io.vertx.core.*;
import io.vertx.core.streams.WriteStream;

public interface NatsClient extends WriteStream<Message> {

    static NatsClient create(NatsOptions natsOptions) {
        return new NatsClientImpl(natsOptions.getNatsBuilder(), natsOptions.getVertx());
    }

    Future<Void> connect();

    Future<NatsStream> jetStream();

    Future<NatsStream> jetStream(JetStreamOptions options);

    @Override
    NatsClient drainHandler(Handler<Void> handler);

    void publish(Message data, Handler<AsyncResult<Void>> handler);

    Future<Void> publish(Message data);

    Future<Void> publish(String subject, String replyTo, String message);

    Future<Void> publish(String subject, String replyTo, byte[] message);

    Future<Void> publish(String subject, String message);

    Future<Void> publish(String subject, byte[] message);

    void request(Message data, Handler<AsyncResult<Message>> handler);

    Future<Message> request(Message data);

    Future<Message> request(String subject, String message);

    Future<Message> request(String subject, byte[] message);


    Future<Void> subscribe(String subject, Handler<Message> handler);

    Future<Void> subscribe(String subject, String queue, Handler<Message> handler);

    Connection getConnection();
}
