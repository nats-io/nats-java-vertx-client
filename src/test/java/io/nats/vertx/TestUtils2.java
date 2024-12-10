package io.nats.vertx;

import io.nats.ConsoleOutput;
import io.nats.NatsServerRunner;
import io.nats.client.*;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

public class TestUtils2 {

    public static String unique() {
        return new NUID().nextSequence();
    }

    public static String unique(int i) {
        return unique() + i;
    }

    public static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { /* ignored */ }
    }

    public static boolean waitUntilStatus(Connection conn, long millis, Connection.Status waitUntilStatus) {
        long times = (millis + 99) / 100;
        for (long x = 0; x < times; x++) {
            sleep(100);
            if (conn.getStatus() == waitUntilStatus) {
                return true;
            }
        }
        return false;
    }

    public static NatsStream jetStream(NatsClient natsClient) {
        return TestUtils.jetStream(natsClient);
    }

    public static NatsClient natsClient(int port) {
        return natsClient(port, e-> {});
    }

    public static NatsClient natsClient(int port, Handler<Throwable> exceptionHandler) {
        final NatsOptions natsOptions = new NatsOptions();
        natsOptions.setExceptionHandler(exceptionHandler);
        natsOptions.setNatsBuilder(new Options.Builder().connectionTimeout(Duration.ofSeconds(5)));
        natsOptions.getNatsBuilder().server("localhost:" + port);

        final NatsClient natsClient = NatsClient.create(natsOptions);
        final Future<Void> connect = natsClient.connect();

        natsClient.setWriteQueueMaxSize(100);
        natsClient.writeQueueFull();
        natsClient.drainHandler(event -> {});

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        connect.onSuccess(event -> {
            latch.countDown();
        }).onFailure(event -> {
            error.set(event);
            latch.countDown();
        });
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (error.get() != null) {
            throw new IllegalStateException(error.get());
        }
        return natsClient;
    }

    public static class TestsRunner {
        NatsServerRunner natsServerRunner;
        Connection nc;
        int port;

        public TestsRunner(NatsServerRunner natsServerRunner) {
            this.natsServerRunner = natsServerRunner;
            port = natsServerRunner.getPort();
        }

        public void close() {
            closeConnection();

            try {
                if (natsServerRunner != null) {
                    natsServerRunner.close();
                }
            }
            catch (Exception e) {
                System.err.println("Closing Test Server Helper Runner: " + e);
            }
        }

        private void closeConnection() {
            try {
                if (nc != null) {
                    nc.close();
                }
            }
            catch (Exception e) {
                System.err.println("Closing Test Server Helper Connection: " + e);
            }
        }
    }

    public static TestsRunner startServer() throws Exception {
        NatsServerRunner.setDefaultOutputSupplier(ConsoleOutput::new);
        NatsServerRunner.setDefaultOutputLevel(Level.WARNING);
        TestsRunner tr = new TestsRunner(new NatsServerRunner(-1, false, true));

        for (int i = 0; i < 5; i++) {
            Options.Builder builder = new Options.Builder()
                .connectionTimeout(Duration.ofSeconds(5))
                .server("localhost:" + tr.port)
                .errorListener(new ErrorListener() {})
                .connectionListener((conn, type) -> {});

            Connection conn = Nats.connect(builder.build());
            if (waitUntilStatus(conn, 5000, Connection.Status.CONNECTED)) {
                tr.nc = conn;
                break;
            }
            conn.close();
        }
        return tr;
    }

}
