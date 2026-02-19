package io.nats.vertx.impl;

import io.nats.client.Connection;
import io.nats.client.Options;
import io.nats.vertx.NatsOptions;

/**
 * Test subclass of {@link NatsClientImpl} that injects a configurable delay
 * into the worker thread's connection assignment path.
 *
 * <h2>Purpose</h2>
 *
 * Widens the race window between the ConnectionListener callback thread
 * (which completes connectFuture via promise.complete()) and the Vert.x
 * worker thread (which calls connection.set() after Nats.connect() returns).
 * On a fast local machine this window is sub-microsecond, making the race
 * impossible to trigger naturally. This subclass widens it to a configurable
 * duration.
 *
 * <h2>How it works</h2>
 *
 * Overrides the package-private {@link NatsClientImpl#onWorkerConnectionEstablished}
 * hook to add a {@code Thread.sleep()} before delegating to super. This
 * delays the worker thread's {@code connection.set(conn)} call while the
 * callback thread has already (or is about to) complete the connect promise.
 *
 * <h2>Behavior with and without the fix</h2>
 *
 * <ul>
 *   <li><b>With fix</b> (connection.set(conn) before promise.complete() in
 *       wireConnectListener): The callback thread sets connection before
 *       completing the promise. The compose chain fires and sees non-null
 *       connection. The delayed worker set is a benign double-set.
 *       <b>Test passes.</b></li>
 *   <li><b>Without fix</b> (only promise.complete() in wireConnectListener):
 *       The callback thread completes the promise without setting connection.
 *       The compose chain fires, reads connection.get() as null (worker is
 *       still sleeping). NatsImpl constructor throws NPE.
 *       <b>Test fails -- bug reproduced.</b></li>
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <pre>
 * NatsOptions opts = ...;
 * NatsClient client = new DelayedNatsClientImpl(
 *     opts.getNatsBuilder(), opts, 200);
 * client.connect()
 *     .compose(v -&gt; client.jetStream())  // NPE without fix
 *     ...
 * </pre>
 */
public class DelayedNatsClientImpl extends NatsClientImpl {

    private final long delayMs;

    /**
     * @param config     Options builder (same as NatsClientImpl constructor)
     * @param natsOptions Vert.x/NATS options
     * @param delayMs    milliseconds to sleep before connection.set() on
     *                   the worker thread
     */
    public DelayedNatsClientImpl(Options.Builder config, NatsOptions natsOptions, long delayMs) {
        super(config, natsOptions);
        this.delayMs = delayMs;
    }

    /**
     * Delays the worker thread's connection assignment by {@link #delayMs}
     * milliseconds, then delegates to the real implementation.
     *
     * <p>This runs on the Vert.x worker thread inside the executeBlocking
     * handler of connect(). The delay simulates thread scheduling pressure
     * that allows the callback thread to complete the connect promise before
     * this worker sets the connection reference.</p>
     */
    @Override
    void onWorkerConnectionEstablished(Connection conn) {
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        super.onWorkerConnectionEstablished(conn);
    }
}
