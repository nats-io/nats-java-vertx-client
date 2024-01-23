package io.nats.client.impl;

import io.nats.client.MessageHandler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;

public class VertxDispatcherFactory extends DispatcherFactory {
    private final Vertx vertx;
    private final ContextInternal context;

    public VertxDispatcherFactory(Vertx vertx) {
        this.vertx = vertx;
        this.context = null;
    }

    @Deprecated
    public VertxDispatcherFactory(ContextInternal context) {
        this.vertx = null;
        this.context = context;
    }

    @Override
    NatsDispatcher createDispatcher(NatsConnection natsConnection, MessageHandler messageHandler) {
        return new VertxDispatcher(natsConnection, messageHandler, vertx == null ? context : (ContextInternal)vertx.getOrCreateContext());
    }
}
