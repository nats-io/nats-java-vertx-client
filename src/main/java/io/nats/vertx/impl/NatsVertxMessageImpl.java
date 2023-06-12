package io.nats.vertx.impl;

import io.nats.client.Message;
import io.nats.vertx.NatsVertxMessage;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;

import java.util.List;
import java.util.stream.Collectors;

public class NatsVertxMessageImpl implements NatsVertxMessage {

    public static List<NatsVertxMessage> listOf(List<Message> messages, ContextInternal contextInternal) {
        return messages.stream().map(m -> new NatsVertxMessageImpl( m, contextInternal)).collect(Collectors.toList());
    }

    private final Message event;
    private final ContextInternal contextInternal;

    public NatsVertxMessageImpl(Message event, ContextInternal contextInternal) {
        this.event = event;
        this.contextInternal = contextInternal;
    }


    @Override
    public Message message() {
        return event;
    }

    @Override
    public ContextInternal context() {
        return contextInternal;
    }
}
