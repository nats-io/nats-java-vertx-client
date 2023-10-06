package io.nats.client.impl;

import io.nats.client.MessageHandler;
import io.vertx.core.Vertx;

public class VertxDispatcher extends NatsDispatcher {
    private final VertxDispatcherMessageQueue vertxIncoming;

    VertxDispatcher(NatsConnection conn, MessageHandler handler, Vertx vertx) {
        super(conn, handler);
        vertxIncoming = new VertxDispatcherMessageQueue(this, vertx);
    }

    @Override
    public void start(String id) {
        internalStart(id, false);
    }

    @Override
    MessageQueue getMessageQueue() {
        return vertxIncoming;
    }
}
