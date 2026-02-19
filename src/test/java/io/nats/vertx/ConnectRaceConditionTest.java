package io.nats.vertx;

import io.nats.NatsServerRunner;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.vertx.impl.NatsClientImpl;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the connect/jetStream race condition in NatsClientImpl.
 *
 * <h2>The bug</h2>
 *
 * {@code NatsClientImpl.wireConnectListener()} wires {@code connectFuture}
 * to the ConnectionListener CONNECTED callback. jnats dispatches this
 * callback asynchronously via {@code callbackExecutor} (separate thread
 * pool). Meanwhile, {@code connect()} calls
 * {@code connection.set(Nats.connect(options))} on a Vert.x worker thread
 * after {@code Nats.connect()} returns. These are independent threads with
 * no ordering guarantee.
 *
 * <p>When the callback thread completes connectFuture before the worker
 * thread runs {@code connection.set()}, any {@code .compose()} callback
 * (e.g., {@code jetStream()}) reads {@code connection.get()} as null,
 * causing an NPE in NatsImpl's constructor.</p>
 *
 * <h2>Reproduction strategy</h2>
 *
 * On a fast local machine, the race window is sub-microsecond: the worker
 * thread's {@code connection.set()} runs as the very next instruction after
 * {@code Nats.connect()} returns. The compose chain must traverse the Vert.x
 * event loop and an {@code executeBlocking} dispatch before reading
 * {@code connection.get()}, which is enough time for the worker to finish.
 *
 * <p>To deterministically reproduce the race, these tests override
 * {@code NatsClientImpl.connect()} to inject a
 * 200ms delay before the worker thread's {@code connection.set()} call.
 * This widens the race window so the compose chain fires while
 * {@code connection.get()} is still null.</p>
 *
 * Since there is limited access, reflection is used. This in itself probably adds delay
 *
 * <p>The delay only affects the worker thread path in {@code connect()},
 * not the callback thread path in {@code wireConnectListener()}. This
 * means:</p>
 *
 * <ul>
 *   <li><b>With the fix</b> ({@code connection.set(conn)} before
 *       {@code promise.complete()} in wireConnectListener): The callback
 *       thread sets connection before completing the promise. The compose
 *       chain fires and sees a non-null connection. The delayed worker
 *       set is a benign double-set. <b>Tests pass.</b></li>
 *   <li><b>Without the fix</b> (only {@code promise.complete()} in
 *       wireConnectListener): The callback thread completes the promise
 *       without setting connection. The compose chain fires, reads
 *       {@code connection.get()} as null (worker is still sleeping), and
 *       NatsImpl's constructor throws NPE. <b>Tests fail.</b></li>
 * </ul>
 */
@ExtendWith(VertxExtension.class)
public class ConnectRaceConditionTest {

    /**
     * Delay injected into the worker thread's connection.set() path.
     * Must be long enough for the compose chain to fire and the
     * executeBlocking handler to read connection.get() before the
     * worker completes. 200ms is well above the ~1ms event loop +
     * worker dispatch latency.
     */
    private static final long WORKER_DELAY_MS = 200;

    NatsServerRunner natsServerRunner;
    int port;

    @BeforeEach
    public void setup() throws Exception {
        NatsServerRunner.setDefaultOutputLevel(Level.WARNING);
        natsServerRunner = NatsServerRunner.builder().jetstream().build();
        port = natsServerRunner.getPort();
        TestUtils2.sleep(500);
    }

    @AfterEach
    public void teardown() throws Exception {
        if (natsServerRunner != null) {
            natsServerRunner.close();
        }
    }

    /**
     * Overrides connect and inserts a delay
     * thread's connection.set() by {@link #WORKER_DELAY_MS}.
     */
    private NatsClient createDelayedClient(Vertx vertx) {
        final NatsOptions natsOptions = new NatsOptions();
        natsOptions.setVertx(vertx);
        natsOptions.setExceptionHandler(e -> {});
        natsOptions.getNatsBuilder()
            .server("nats://localhost:" + port)
            .connectionTimeout(Duration.ofSeconds(5));

        return new NatsClientImpl(natsOptions.getNatsBuilder(), natsOptions) {
            @Override
            public Future<Void> connect() {
                ContextInternal context = (ContextInternal) vertx.getOrCreateContext();

                    context.executeBlocking(event -> {
                    try {
                        Field field = NatsClientImpl.class.getDeclaredField("connection");
                        field.setAccessible(true);
                        AtomicReference<Connection> _connection = (AtomicReference<Connection>) field.get(this);

                        field = NatsClientImpl.class.getDeclaredField("options");
                        field.setAccessible(true);
                        Options _options = (Options)field.get(this);

                        Connection connection = Nats.connect(_options);
                        try {
                            Thread.sleep(WORKER_DELAY_MS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw e;
                        }
                        _connection.compareAndSet(null, connection);

                    } catch (Exception e) {
                        Promise<Void> _connectFuture;
                        try {
                            Field field = NatsClientImpl.class.getDeclaredField("connectFuture");
                            field.setAccessible(true);
                            _connectFuture = (Promise<Void>)field.get(this);
                        }
                        catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }

                        try {
                            Method method = NatsClientImpl.class.getDeclaredMethod("handleException", Promise.class, Exception.class);
                            method.setAccessible(true);
                            method.invoke(this, _connectFuture, e);
                        }
                        catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }, false);

                try {
                    Field field = NatsClientImpl.class.getDeclaredField("periodicFlush");
                    field.setAccessible(true);
                    boolean _periodicFlush = field.getBoolean(this);

                    field = NatsClientImpl.class.getDeclaredField("periodicFlushInterval");
                    field.setAccessible(true);
                    long _periodicFlushInterval = field.getLong(this);

                    if (_periodicFlush) {
                        context.setTimer(_periodicFlushInterval, event -> {
                            try {
                                Method method = NatsClientImpl.class.getDeclaredMethod("runFlush");
                                method.setAccessible(true);
                                method.invoke(this);
                            }
                            catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }

                Promise<Void> _connectFuture;
                try {
                    Field field = NatsClientImpl.class.getDeclaredField("connectFuture");
                    field.setAccessible(true);
                    _connectFuture = (Promise<Void>)field.get(this);
                    return _connectFuture.future();
                }
                catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        } ;
    }

    // -- Tests ---------------------------------------------------------------

    /**
     * Deterministic reproduction: {@code connect().compose(v -> jetStream())}
     * with a delayed worker-side connection.set().
     *
     * <p>With the fix, the callback thread sets connection before completing
     * the promise, so jetStream() in the compose chain sees a non-null
     * connection regardless of the worker delay. Without the fix, the
     * compose chain fires immediately after promise.complete() and
     * jetStream() reads null, triggering an NPE in NatsImpl's
     * constructor.</p>
     */
    @Test
    public void testConnectComposeJetStreamWithDelay() throws Exception {
        final int iterations = 10;
        final AtomicInteger successes = new AtomicInteger(0);
        final AtomicInteger failures = new AtomicInteger(0);
        final AtomicReference<Throwable> firstError = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(iterations);

        for (int i = 0; i < iterations; i++) {
            final Vertx vertx = Vertx.vertx();
            final NatsClient client = createDelayedClient(vertx);

            client.connect()
                .compose(v -> client.jetStream())
                .onSuccess(stream -> {
                    successes.incrementAndGet();
                    latch.countDown();
                    try { client.close(); } catch (Exception ignored) {}
                    vertx.close();
                })
                .onFailure(err -> {
                    failures.incrementAndGet();
                    firstError.compareAndSet(null, err);
                    latch.countDown();
                    try { client.close(); } catch (Exception ignored) {}
                    vertx.close();
                });
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS), "Timed out waiting for iterations");

        if (firstError.get() != null) {
            fail("Race condition triggered: " + failures.get() + "/" + iterations
                + " failures. First error: " + firstError.get().getMessage(),
                firstError.get());
        }

        assertEquals(iterations, successes.get(),
            "All iterations should succeed (got " + failures.get() + " failures)");
    }

    /**
     * Deterministic reproduction for the {@code keyValue()} path.
     *
     * <p>Same mechanism as {@link #testConnectComposeJetStreamWithDelay()}
     * but exercises keyValue(), which has the same race:
     * NatsVertxKeyValueImpl's constructor (via NatsImpl) dereferences the
     * null connection.</p>
     *
     * <p>Non-NPE failures (e.g., bucket not found) are expected and do not
     * indicate the race condition -- only NPE does.</p>
     */
    @Test
    public void testConnectComposeKeyValueWithDelay() throws Exception {
        final int iterations = 10;
        final AtomicInteger npeFailures = new AtomicInteger(0);
        final AtomicReference<Throwable> firstNpe = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(iterations);

        for (int i = 0; i < iterations; i++) {
            final Vertx vertx = Vertx.vertx();
            final NatsClient client = createDelayedClient(vertx);

            client.connect()
                .compose(v -> client.keyValue("test-bucket"))
                .onSuccess(kv -> {
                    latch.countDown();
                    try { client.close(); } catch (Exception ignored) {}
                    vertx.close();
                })
                .onFailure(err -> {
                    if (err instanceof NullPointerException) {
                        npeFailures.incrementAndGet();
                        firstNpe.compareAndSet(null, err);
                    }
                    // Non-NPE failures (bucket not found, IOException) are
                    // expected and acceptable -- only NPE indicates the race.
                    latch.countDown();
                    try { client.close(); } catch (Exception ignored) {}
                    vertx.close();
                });
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS), "Timed out waiting for iterations");

        if (firstNpe.get() != null) {
            fail("Race condition triggered (NPE): " + npeFailures.get() + "/" + iterations
                + " NPE failures. First: " + firstNpe.get().getMessage(),
                firstNpe.get());
        }

        assertEquals(0, npeFailures.get(), "No NPE failures expected");
    }

    /**
     * Structural validation: {@code getConnection()} must be non-null
     * inside a compose() callback chained on connect(), even with a
     * delayed worker-side connection.set().
     *
     * <p>This directly tests the invariant the fix guarantees without
     * involving NatsImpl's constructor, isolating the race from other
     * failure modes. If this test fails, the fix is missing or
     * incomplete.</p>
     */
    @Test
    public void testConnectionNonNullInComposeWithDelay() throws Exception {
        final int iterations = 10;
        final AtomicInteger nullCount = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(iterations);

        for (int i = 0; i < iterations; i++) {
            final Vertx vertx = Vertx.vertx();
            final NatsClient client = createDelayedClient(vertx);

            client.connect()
                .compose(v -> {
                    if (client.getConnection() == null) {
                        nullCount.incrementAndGet();
                    }
                    latch.countDown();
                    vertx.close();
                    return io.vertx.core.Future.succeededFuture();
                })
                .onFailure(err -> {
                    latch.countDown();
                    vertx.close();
                });
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS), "Timed out waiting for iterations");

        assertEquals(0, nullCount.get(),
            "connection must be non-null in compose() callback (with " + WORKER_DELAY_MS
            + "ms worker delay). Got null " + nullCount.get() + "/" + iterations
            + " times. This indicates connection.set(conn) is not called before "
            + "promise.complete() in wireConnectListener().");
    }
}
