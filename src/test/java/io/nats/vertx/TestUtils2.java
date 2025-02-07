package io.nats.vertx;

import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.NUID;
import io.nats.client.Options;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TestUtils2 {

    public static String unique() {
        return new NUID().nextSequence();
    }

    public static String unique(String prefix) {
        return prefix + unique();
    }

    public static String unique(int i) {
        return unique() + i;
    }

    public static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { /* ignored */ }
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
        natsOptions.getNatsBuilder().server("nats://localhost:" + port);

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

    public static StreamInfo createStream(JetStreamManagement jsm, String streamName, String... subjects) throws IOException, JetStreamApiException {
        try {
            jsm.deleteStream(streamName);
        }
        catch (Exception ignore) {}

        StreamConfiguration sc = StreamConfiguration.builder()
            .name(streamName)
            .storageType(StorageType.Memory)
            .subjects(subjects)
            .build();
        return jsm.addStream(sc);
    }
}
