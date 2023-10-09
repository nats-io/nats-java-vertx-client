package io.nats.client.impl;

import io.nats.client.MessageHandler;
import io.vertx.core.impl.ContextInternal;

public class VertxDispatcherFactory extends DispatcherFactory {
    private final ContextInternal context;

    public VertxDispatcherFactory(ContextInternal context) {
        this.context = context;
    }

    @Override
    NatsDispatcher createDispatcher(NatsConnection natsConnection, MessageHandler messageHandler) {
        return new VertxDispatcher(natsConnection, messageHandler, context);
    }
}
