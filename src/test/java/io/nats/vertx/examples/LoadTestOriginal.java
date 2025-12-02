package io.nats.vertx.examples;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class LoadTestOriginal {

    public final static String SUBJECT_NAME = "loadTestTopic";


    public final static int PORT = 4222;

    public final static int MESSAGE_COUNT = 400_000;

    public final static int WARM_UP = 1;

    public final static int RUNS = 20;


    public static void main(String[] args) throws Exception {



        setup(PORT);
        final Connection natsClient = getNatsClient(PORT);
        final JetStream jetStream = getJetStream(natsClient);


        System.out.println("WARM UP");
        for (int i =0; i < WARM_UP; i++) {
            loadTest(jetStream, natsClient);
        }

        System.out.println("RUNS");
        for (int i =0; i < RUNS; i++) {
            loadTest(jetStream, natsClient);
            Thread.sleep(1000);
        }

        closeClient(natsClient);

    }

    private static void loadTest(final JetStream jetStream, final Connection connection) throws Exception {
        final CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);
        final LongAdder counter = new LongAdder();

        final Dispatcher dispatcher = connection.createDispatcher();

        JetStreamSubscription subscription = jetStream.subscribe(SUBJECT_NAME, dispatcher, event -> {

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


        connection.closeDispatcher(dispatcher);

    }

    static void closeClient(Connection natsClient) throws Exception {
        natsClient.drain(Duration.ofSeconds(10)).get();
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

    static JetStream getJetStream(final Connection natsClient) throws Exception {
        return natsClient.jetStream();
    }

    static Connection getNatsClient(int port) throws Exception {
        final Options.Builder natsOptions = new Options.Builder().connectionTimeout(Duration.ofSeconds(5));
        final CountDownLatch latch = new CountDownLatch(1);

        natsOptions.server("localhost:" + port).connectionListener(new ConnectionListener() {
            @Override
            public void connectionEvent(Connection conn, Events type) {
                System.out.println("Connection EVENT " + type);
                if (type == Events.CONNECTED) {
                    latch.countDown();
                }
            }
        });
        final Connection natsClient = Nats.connect(natsOptions.build());

        latch.await(1, TimeUnit.SECONDS);

        return natsClient;
    }
}
