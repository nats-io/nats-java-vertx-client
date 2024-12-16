package io.nats.vertx.impl;

import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.vertx.core.*;
import io.vertx.core.impl.ContextInternal;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * NATS base implementation
 */
public class NatsImpl {

    public final Vertx vertx;

    protected final ConcurrentHashMap<String, Dispatcher> dispatcherMap = new ConcurrentHashMap<>();
    protected final ConcurrentHashMap<String, JetStreamSubscription> subscriptionMap = new ConcurrentHashMap<>();

    public final Connection conn;
    public final Duration timeout;
    public final JetStreamOptions jso;
    public final JetStreamManagement jsm;
    public final JetStream js;

    protected final AtomicReference<Handler<Throwable>> exceptionHandler = new AtomicReference<>();

    protected NatsImpl(final Connection conn,
                       final Vertx vertx,
                       final Handler<Throwable> exceptionHandler,
                       final JetStreamOptions jso)
    {
        this.conn = conn;
        this.timeout = jso == null || jso.getRequestTimeout() == null ? conn.getOptions().getConnectionTimeout() : jso.getRequestTimeout();
        this.jso = JetStreamOptions.builder(jso).requestTimeout(this.timeout).build();

        try {
            this.jsm = conn.jetStreamManagement(this.jso);
            this.js = jsm.jetStream();
        }
        catch (IOException e) {
            if (exceptionHandler != null) {
                exceptionHandler.handle(e);
            }
            throw new RuntimeException(e);
        }
        this.vertx = vertx;
        this.exceptionHandler.set(exceptionHandler);
    }

    public ContextInternal context() {
        return (ContextInternal)vertx.getOrCreateContext();
    }

    protected void endImpl(Handler<AsyncResult<Void>> handler) {
        //No Op
        final Promise<Void> promise = context().promise();
        handler.handle(promise.future());
    }

    public <T> Future<T> executeUnorderedBlocking(Callable<T> callable) {
        return context().executeBlocking(() -> {
            try {
                return callable.call();
            }
            catch (Exception e) {
                exceptionHandler.get().handle(e);
                throw e;
            }
        }, false);
    }

    protected void visitSubject(String streamName, String subject, DeliverPolicy deliverPolicy, boolean headersOnly, boolean ordered, MessageHandler handler) throws IOException, JetStreamApiException {
        visitSubject(streamName, Collections.singletonList(subject), deliverPolicy, headersOnly, ordered, handler);
    }

    protected void visitSubject(String streamName, List<String> subjects, DeliverPolicy deliverPolicy, boolean headersOnly, boolean ordered, MessageHandler handler) throws IOException, JetStreamApiException {
        ConsumerConfiguration.Builder ccb = ConsumerConfiguration.builder()
            .ackPolicy(AckPolicy.None)
            .deliverPolicy(deliverPolicy)
            .headersOnly(headersOnly)
            .filterSubjects(subjects);

        PushSubscribeOptions pso = PushSubscribeOptions.builder()
            .stream(streamName)
            .ordered(ordered)
            .configuration(ccb.build())
            .build();

        JetStreamSubscription sub = js.subscribe(null, pso);
        try {
            boolean lastWasNull = false;
            long pending = sub.getConsumerInfo().getCalculatedPending();
            while (pending > 0) { // no need to loop if nothing pending
                Message m = sub.nextMessage(timeout);
                if (m == null) {
                    if (lastWasNull) {
                        return; // two timeouts in a row is enough
                    }
                    lastWasNull = true;
                }
                else {
                    handler.onMessage(m);
                    if (--pending == 0) {
                        return;
                    }
                    lastWasNull = false;
                }
            }
        } catch (Exception e) {
            exceptionHandler.get().handle(e);
        }
        finally {
            sub.unsubscribe();
        }
    }
}
