package io.nats.vertx;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.fail;

public class LoadTestMain {

    public final static String SUBJECT_NAME = "loadTestTopic";

    public final static byte[] DATA = "0".getBytes(StandardCharsets.UTF_8);

    public final static int PORT = 4222;

    public final static int MESSAGE_COUNT = 400_000;

    public final static int WARM_UP = 1;

    public final static int RUNS = 20;

    public final static Vertx VERTX = Vertx.vertx();

    public static void main(String[] args) throws Exception {



        setup(PORT);
        final NatsClient natsClient = getNatsClient(PORT);
        final NatsStream jetStream = getJetStream(natsClient);


        System.out.println("WARM UP");
        for (int i =0; i < WARM_UP; i++) {
            loadTest(jetStream);
        }

        System.out.println("RUNS");
        for (int i =0; i < RUNS; i++) {
            loadTest(jetStream);
            Thread.sleep(1000);
        }

        closeClient(natsClient);

        closeVertx();
    }

    private static void loadTest(final NatsStream jetStream) throws Exception {
        final CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);
        final LongAdder counter = new LongAdder();

        jetStream.subscribe(SUBJECT_NAME, event -> {

            latch.countDown();
            counter.increment();
        }, true, PushSubscribeOptions.builder().build());

        final long startTime = System.currentTimeMillis();

        boolean await = latch.await(30, TimeUnit.SECONDS);
        if (await) {
            final long endTime = System.currentTimeMillis();
            final long duration = endTime - startTime;
            System.out.printf("%s %d %,d %d \n", SUBJECT_NAME, MESSAGE_COUNT, counter.sum(), duration);
        } else {
            System.out.printf("FAIL %s %d %d \n", SUBJECT_NAME, MESSAGE_COUNT, counter.sum());
        }


        unsubscribe(jetStream);
    }

    private static void unsubscribe(final NatsStream jetStream) throws Exception{
        final CountDownLatch latch = new CountDownLatch(1);


        jetStream.unsubscribe(SUBJECT_NAME).onSuccess(handler->latch.countDown());

        latch.await(5, TimeUnit.SECONDS);
    }

    private static void closeVertx() throws Exception{
        final CountDownLatch latch = new CountDownLatch(1);

        VERTX.close().onSuccess(handler-> {
            latch.countDown();
        });

        latch.await(5, TimeUnit.SECONDS);
    }

    static void closeClient(NatsClient natsClient) throws InterruptedException {
        final CountDownLatch endLatch = new CountDownLatch(1);
        natsClient.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    public static void setup(int port) throws Exception {


        Options.Builder builder = new Options.Builder().connectionTimeout(Duration.ofSeconds(5))
                .server("localhost:" + port);
        Connection nc = Nats.connect(builder.build());
        JetStreamManagement jsm = nc.jetStreamManagement();
        StreamInfo streamInfo = null;

        try {
            streamInfo = jsm.getStreamInfo(SUBJECT_NAME);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        if (streamInfo == null) {
            StreamConfiguration sc = StreamConfiguration.builder().name(SUBJECT_NAME).storageType(StorageType.Memory).build();
            // Add or use an existing stream.
            StreamInfo streamInfo1 = jsm.addStream(sc);
        }

        nc.close();

    }

    static NatsStream getJetStream(final NatsClient natsClient) throws InterruptedException {
        Future<NatsStream> connect = natsClient.jetStream();


        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicReference<NatsStream> stream = new AtomicReference<>();
        connect.onSuccess(event -> {

            // Call no op methods.
            event.drainHandler(event1 -> {
            });
            event.setWriteQueueMaxSize(100);
            event.writeQueueFull();
            event.end(endEvent -> {
            });
            stream.set(event);
            latch.countDown();
        }).onFailure(event -> {
            error.set(event);
            latch.countDown();
        });
        latch.await(1, TimeUnit.SECONDS);
        if (error.get() != null) {
            fail();
        }
        return stream.get();
    }

    static NatsClient getNatsClient(int port) throws InterruptedException {
        final NatsOptions natsOptions = new NatsOptions();
        natsOptions.setVertx(VERTX);
        natsOptions.setNatsBuilder(new Options.Builder());
        natsOptions.getNatsBuilder().server("localhost:" + port).connectionListener(new ConnectionListener() {
            @Override
            public void connectionEvent(Connection conn, Events type) {
                System.out.println("Connection EVENT " + type);
            }
        });
        final NatsClient natsClient = NatsClient.create(natsOptions);
        final Future<Void> connect = natsClient.connect();

        natsClient.exceptionHandler(Throwable::printStackTrace);

        //No op methods
        natsClient.setWriteQueueMaxSize(100);
        natsClient.writeQueueFull();
        natsClient.drainHandler(event -> {
        });

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        connect.onSuccess(event -> {
            latch.countDown();
        }).onFailure(event -> {
            error.set(event);
            latch.countDown();
        });
        latch.await(1, TimeUnit.SECONDS);
        if (error.get() != null) {
            throw new IllegalStateException(error.get());
        }
        return natsClient;
    }
}
