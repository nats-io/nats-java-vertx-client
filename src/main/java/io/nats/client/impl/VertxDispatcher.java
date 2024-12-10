package io.nats.client.impl;

import io.nats.client.MessageHandler;
import io.vertx.core.impl.ContextInternal;

public class VertxDispatcher extends NatsDispatcher {
    private final VertxDispatcherMessageQueue vertxIncoming;

    VertxDispatcher(NatsConnection conn, MessageHandler handler, ContextInternal context) {
        super(conn, handler);
        vertxIncoming = new VertxDispatcherMessageQueue(this, context, conn);
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
