package io.nats.vertx;

import io.nats.client.Options;
import io.vertx.core.Vertx;

/** Holds the NATS options. */
public class NatsOptions {
    private Options.Builder natsBuilder;
    private Vertx vertx;

    private boolean periodicFlush;
    private long periodicFlushInterval;

    /** Get the NATS builder.
     *
     * @return Options.Builder
     */
    public Options.Builder getNatsBuilder() {
        if (natsBuilder == null) {
            natsBuilder = new Options.Builder();
        }
        return natsBuilder;
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
            vertx = Vertx.vertx();
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
}
