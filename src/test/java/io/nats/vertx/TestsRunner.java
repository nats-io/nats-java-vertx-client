package io.nats.vertx;

import io.nats.ConsoleOutput;
import io.nats.NatsServerRunner;
import io.nats.client.*;

import java.time.Duration;
import java.util.logging.Level;

import static io.nats.vertx.TestUtils2.sleep;

public class TestsRunner {
    NatsServerRunner natsServerRunner;
    int port;
    Connection nc;
    JetStreamManagement jsm;
    JetStream js;

    public static TestsRunner instance() throws Exception {
        NatsServerRunner.setDefaultOutputSupplier(ConsoleOutput::new);
        NatsServerRunner.setDefaultOutputLevel(Level.WARNING);
        TestsRunner tr = new TestsRunner(NatsServerRunner.builder().jetstream().build());

        for (int i = 0; i < 5; i++) {
            Options.Builder builder = new Options.Builder()
                .connectionTimeout(Duration.ofSeconds(5))
                .server("nats://localhost:" + tr.port)
                .errorListener(new ErrorListener() {})
                .connectionListener((conn, type) -> {});

            Connection conn = Nats.connect(builder.build());
            if (waitUntilStatus(conn, 5000, Connection.Status.CONNECTED)) {
                tr.nc = conn;
                tr.jsm = conn.jetStreamManagement();
                tr.js = conn.jetStream();
                break;
            }
            conn.close();
        }
        return tr;
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

    private TestsRunner(NatsServerRunner natsServerRunner) {
        this.natsServerRunner = natsServerRunner;
        port = natsServerRunner.getPort();
    }

    public void close() {
        try {
            if (nc != null) {
                nc.close();
            }
        }
        catch (Exception e) {
            System.err.println("Closing Test Server Helper Connection: " + e);
        }

        try {
            if (natsServerRunner != null) {
                natsServerRunner.close();
            }
        }
        catch (Exception e) {
            System.err.println("Closing Test Server Helper Runner: " + e);
        }
    }
}
