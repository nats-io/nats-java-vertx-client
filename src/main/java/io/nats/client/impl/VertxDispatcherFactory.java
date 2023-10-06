package io.nats.client.impl;

import io.nats.client.MessageHandler;
import io.vertx.core.Vertx;

public class VertxDispatcherFactory extends DispatcherFactory {
    private final Vertx vertx;

    public VertxDispatcherFactory(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    NatsDispatcher createDispatcher(NatsConnection natsConnection, MessageHandler messageHandler) {
        return new VertxDispatcher(natsConnection, messageHandler, vertx);
    }
}
