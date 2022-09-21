package io.nats.vertx;


import io.nats.client.Connection;
        import io.nats.client.JetStreamOptions;
        import io.nats.client.Message;
import io.nats.client.Options;
import io.vertx.core.AsyncResult;
        import io.vertx.core.Future;
        import io.vertx.core.Handler;
        import io.vertx.core.Vertx;
        import io.vertx.core.streams.WriteStream;

public interface NatsClient extends WriteStream<Message> {

    static NatsClient create(Options config) {
        return new NatsClientImpl( config);
    }

    Future<Void> connect();

    Future<NatsStream> jetStream();

    Future<NatsStream> jetStream(JetStreamOptions options);

    @Override
    NatsClient drainHandler(Handler<Void> handler);

    void publish(Message data, Handler<AsyncResult<Void>> handler);

    Future<Void> publish(Message data);

    Future<Void> publish(String subject, String replyTo, String message);

    Future<Void> publish(String subject, String message);

    void request(Message data, Handler<AsyncResult<Message>> handler);

    Future<Message> request(Message data);

    Future<Message> request(String subject, String replyTo, String message);

    Future<Message> request(String subject, String message);

    Future<Void> subscribe(String subject, Handler<Message> handler);

    Future<Void> subscribe(String subject, String queue, Handler<Message> handler);

    Connection getConnection();
}
