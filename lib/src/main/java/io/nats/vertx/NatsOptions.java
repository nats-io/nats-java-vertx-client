package io.nats.vertx;

import io.nats.client.Options;
import io.vertx.core.Vertx;

public class NatsOptions {
    private Options.Builder natsBuilder;
    private Vertx vertx;

    public Options.Builder getNatsBuilder() {
        if (natsBuilder == null) {
            natsBuilder = new Options.Builder();
        }
        return natsBuilder;
    }

    public NatsOptions setNatsBuilder(Options.Builder natsBuilder) {
        this.natsBuilder = natsBuilder;
        return this;
    }

    public Vertx getVertx() {
        if (vertx == null) {
            vertx = Vertx.vertx();
        }
        return vertx;
    }

    public NatsOptions setVertx(Vertx vertx) {
        this.vertx = vertx;
        return this;
    }
}
