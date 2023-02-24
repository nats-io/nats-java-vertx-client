package io.nats.vertx;

import io.nats.client.Message;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.time.Duration;

public interface NatsVertxMessage {
    
    
    /** Wrapper around the Nats Message. */
    Message message();

    /** Reference to the vertx vertical where Futures are scheduled. */
    Vertx vertx();

    /**
     * Acknowledge the message as processed successfully.
     * @return Future
     */
    default Future<Void> ack() {
        // This now returns a future that is truly async by running the code in the vertx thread context.
        // Ack by default does not block, but does IO so could introduce a wait state.
        final Promise<Void> promise = Promise.promise();
        vertx().executeBlocking(event -> {
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
    default Future<Void> nak() {
        final Promise<Void> promise = Promise.promise();
        // This now returns a future that is truly async by running the code in the vertx thread context.
        vertx().executeBlocking(event -> {
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
    default Future<Void> nakWithDelay(final Duration nakDelay) {
        // This now returns a future that is truly async by running the code using vertx executeBlocking
        // which executes with a thread pool separate from the event loop IO context of vert.x.
        // because we can't block the vert.x IO event loop.
        final Promise<Void> promise = Promise.promise();
        vertx().executeBlocking(event -> {
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
    default Future<Void> ackWithDelay(final Duration ackDelay) {
        final Promise<Void> promise = Promise.promise();
        // This now returns a future that is truly async by running the code using vertx executeBlocking
        // which executes with a thread pool separate from the event loop IO context of vert.x.
        // because we can't block the vert.x IO event loop.
        vertx().executeBlocking(event -> {
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
    default Future<Void> nakWithDelay(final long nakDelayMillis) {
        return this.nakWithDelay(Duration.ofMillis(nakDelayMillis));
    }

    /**
     * Acknowledge the message as processed successfully.
     * @param nakDelayMillis wait this long for the server to respond that it got the ack.
     * @return Future
     */
    default Future<Void> ackWithDelay(final long nakDelayMillis) {
        return this.ackWithDelay(Duration.ofMillis(nakDelayMillis));
    }
}
