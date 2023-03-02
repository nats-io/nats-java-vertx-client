package io.nats.vertx;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.nats.client.impl.AckType;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsJetStreamMetaData;
import io.nats.client.support.Status;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

public interface NatsVertxMessage extends Message{
    
    
    /** Wrapper around the Nats Message. */
    Message message();

    /** Reference to the vertx vertical where Futures are scheduled.
     */
    Vertx vertx();

    default ContextInternal context() {
        return (ContextInternal)  vertx().getOrCreateContext();
    }

    /**
     * Acknowledge the message as processed successfully.
     * @return Future
     */
    default Future<Void> ackAsync() {
        // This now returns a future that is truly async by running the code in the vertx thread context.
        // Ack by default does not block, but does IO so could introduce a wait state.
        final Promise<Void> promise = Promise.promise();
        context().executeBlocking(event -> {
            try {
                message().ack();
                promise.complete();
            } catch (Throwable e){
                promise.tryFail(e);
            }
        }, false);
        return promise.future();
    }

    /**
     * Tell server that we were not able to process the message.
     * @return Future
     */
    default Future<Void> nakAsync() {
        final Promise<Void> promise = Promise.promise();
        // This now returns a future that is truly async by running the code in the vertx thread context.
        context().executeBlocking(event -> {
            try {
                message().nak();
                promise.complete();
            } catch (Throwable e){
                promise.tryFail(e);
            }
        }, false);
        return promise.future();
    }
    /**
     * Tell server that we were not able to process the message.
     * Block up to the duration time waiting for the message to complete.
     * @param nakDelay wait this long for the server to respond that it got the nak ack.
     * @return Future
     */
    default Future<Void> nakWithDelayAsync(final Duration nakDelay) {
        // This now returns a future that is truly async by running the code using vertx executeBlocking
        // which executes with a thread pool separate from the event loop IO context of vert.x.
        // because we can't block the vert.x IO event loop.
        final Promise<Void> promise = Promise.promise();
        context().executeBlocking(event -> {
            try {
                message().nakWithDelay(nakDelay);
                promise.complete();
            } catch (Throwable e){
                promise.tryFail(e);
            }
        }, false);
        return promise.future();
    }

    /**
     * Acknowledge the message as processed successfully.
     * @param ackDelay wait this long for the server to respond that it got the ack.
     * @return Future
     */
    default Future<Void> ackWithDelayAsync(final Duration ackDelay) {
        final Promise<Void> promise = Promise.promise();
        // This now returns a future that is truly async by running the code using vertx executeBlocking
        // which executes with a thread pool separate from the event loop IO context of vert.x.
        // because we can't block the vert.x IO event loop.
        context().executeBlocking(event -> {
            try {
                message().ackSync(ackDelay);
                promise.complete();
            } catch (Throwable e){
                promise.tryFail(e);
            }
        }, false);
        return promise.future();
    }

    /**
     * Tell server that we were not able to process the message.
     * Block up to the duration time waiting for the message to complete.
     * @param nakDelayMillis wait this long for the server to respond that it got the nak ack.
     * @return Future
     */
    default Future<Void> nakWithDelayAsync(final long nakDelayMillis) {
        return this.nakWithDelayAsync(Duration.ofMillis(nakDelayMillis));
    }

    /**
     * Acknowledge the message as processed successfully.
     * @param nakDelayMillis wait this long for the server to respond that it got the ack.
     * @return Future
     */
    default Future<Void> ackWithDelayAsync(final long nakDelayMillis) {
        return this.ackWithDelayAsync(Duration.ofMillis(nakDelayMillis));
    }

    @Override
    default String getSubject() {
        return message().getSubject();
    }

    @Override
    default String getReplyTo() {
        return message().getReplyTo();
    }

    @Override
    default boolean hasHeaders() {
        return message().hasHeaders();
    }

    @Override
    default Headers getHeaders() {
        return message().getHeaders();
    }

    @Override
    default boolean isStatusMessage() {
        return message().isStatusMessage();
    }

    @Override
    default Status getStatus() {
        return message().getStatus();
    }

    @Override
    default byte[] getData() {
        return message().getData();
    }

    @Override
    default boolean isUtf8mode() {
        return message().isUtf8mode();
    }

    @Override
    default Subscription getSubscription() {
        return message().getSubscription();
    }

    @Override
    default String getSID() {
        return message().getSID();
    }

    @Override
    default Connection getConnection() {
        return message().getConnection();
    }

    @Override
    default NatsJetStreamMetaData metaData() {
        return message().metaData();
    }

    @Override
    default AckType lastAck() {
        return  message().lastAck();
    }

    @Override
    default void ack() {
        message().ack();
    }

    @Override
    default void ackSync(Duration timeout) throws TimeoutException, InterruptedException {
        message().ackSync(timeout);
    }

    @Override
    default void nak()  {
        message().nak();
    }

    @Override
    default void nakWithDelay(Duration nakDelay) {
        message().nakWithDelay(nakDelay);
    }

    @Override
    default void nakWithDelay(long nakDelayMillis) {
        message().nakWithDelay(nakDelayMillis);
    }

    @Override
    default void term() {
        message().term();
    }

    @Override
    default void inProgress() {
        message().term();
    }

    @Override
    default boolean isJetStream() {
        return message().isJetStream();
    }
}
