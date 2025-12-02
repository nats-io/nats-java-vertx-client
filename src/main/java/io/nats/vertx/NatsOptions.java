package io.nats.vertx;

import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.Options;
import io.nats.client.impl.ErrorListenerLoggerImpl;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/** Holds the NATS options. */
public class NatsOptions {
    public static final Duration DEFAULT_NEXT_TIMEOUT = Duration.ofNanos(1);
    public static final long DEFAULT_NO_MESSAGE_DELAY_MILLIS = 100;

    private Options.Builder natsBuilder;
    private Vertx vertx;
    private boolean periodicFlush;
    private long periodicFlushInterval;
    private VertxOptions vertxOptions;
    private Duration nextTimeout = DEFAULT_NEXT_TIMEOUT;
    private long noMessageDelayMillis = DEFAULT_NO_MESSAGE_DELAY_MILLIS;

    private Handler<Throwable> exceptionHandler;

    public Handler<Throwable> getExceptionHandler() {
        return exceptionHandler;
    }

    public NatsOptions setExceptionHandler(Handler<Throwable> exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
        return this;
    }

    public static String hexHashCode(Object conn) {
        return Integer.toHexString(conn.hashCode()).toUpperCase();
    }

    /** Get the NATS builder.
     *
     * @return Options.Builder
     */
    public Options.Builder getNatsBuilder() {
        if (natsBuilder == null) {
            natsBuilder = new Options.Builder().connectionTimeout(Duration.ofSeconds(5));
        }
        configureExceptionHandler();
        return natsBuilder;
    }

    private void configureExceptionHandler() {
        if (getExceptionHandler()!=null) {
            final Handler<Throwable> exceptionHandler = getExceptionHandler();
            if (!(natsBuilder.build().getErrorListener() instanceof ErrorListenerLoggerImpl)) {

                natsBuilder.errorListener(new ErrorListener() {
                    @Override
                    public void errorOccurred(Connection conn, String error) {
                        exceptionHandler.handle(new IllegalStateException(error));
                    }

                    @Override
                    public void exceptionOccurred(Connection conn, Exception exp) {
                        exceptionHandler.handle(new IllegalStateException(exp));
                    }
                });
            } else {
                ErrorListener errorListener = natsBuilder.build().getErrorListener();
                natsBuilder.errorListener(new ErrorListener() {
                    @Override
                    public void errorOccurred(Connection conn, String error) {
                        errorListener.errorOccurred(conn, error);
                        exceptionHandler.handle(new IllegalStateException(error));
                    }

                    @Override
                    public void exceptionOccurred(Connection conn, Exception exp) {
                        errorListener.exceptionOccurred(conn, exp);
                        exceptionHandler.handle(new IllegalStateException(exp));
                    }
                });
            }
        }
    }

    /**
     * Sets the NATS option builder.
     * @param natsBuilder set the nats builder.
     * @return this options
     */
    public NatsOptions setNatsBuilder(Options.Builder natsBuilder) {
        this.natsBuilder = natsBuilder;
        return this;
    }

    /** Get Vert.x associated with this NATS client.
     * @return vert.x instance.
     */
    public Vertx getVertx() {
        if (vertx == null) {
            vertx = Vertx.vertx(vertxOptions);
        }
        return vertx;
    }

    /**
     * Set vert.x
     * @param vertx Vert.x vertical to set.
     * @return this options.
     */
    public NatsOptions setVertx(Vertx vertx) {
        this.vertx = vertx;
        return this;
    }

    public boolean isPeriodicFlush() {
        return periodicFlush;
    }

    public NatsOptions setPeriodicFlush(boolean periodicFlush) {
        this.periodicFlush = periodicFlush;
        return this;
    }

    public long getPeriodicFlushInterval() {
        return periodicFlushInterval;
    }

    public NatsOptions setPeriodicFlushInterval(long periodicFlushInterval) {
        this.periodicFlushInterval = periodicFlushInterval;
        return this;
    }

    public Duration getNextTimeout() {
        return nextTimeout;
    }

    public NatsOptions nextTimeout(Duration nextTimeout) {
        if (nextTimeout == null || nextTimeout.toNanos() < 1 || nextTimeout.isNegative()) {
            this.nextTimeout = DEFAULT_NEXT_TIMEOUT;
        }
        else {
            this.nextTimeout = nextTimeout;
        }
        return this;
    }

    public NatsOptions nextTimeout(long nextTimeout, TimeUnit timeUnit) {
        long nanos = timeUnit.toNanos(nextTimeout);
        this.nextTimeout = nanos < 1
            ? DEFAULT_NEXT_TIMEOUT
            : Duration.ofNanos(nanos);
        return this;
    }

    public long getNoMessageDelayMillis() {
        return noMessageDelayMillis;
    }

    public NatsOptions noMessageDelayMillis(long noMessageDelayMillis) {
        this.noMessageDelayMillis = noMessageDelayMillis < 1
            ? DEFAULT_NO_MESSAGE_DELAY_MILLIS
            : noMessageDelayMillis;
        return this;
    }

    public NatsOptions setVertxOptions(VertxOptions vertxOptions) {
        this.vertxOptions = vertxOptions;
        return this;
    }
}
