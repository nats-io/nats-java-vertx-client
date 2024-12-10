package io.nats.vertx;

import io.nats.NatsServerRunner;
import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class NatsSimplePerfTest {


    final String SUBJECT_NAME = "jetTestSubject";

    NatsServerRunner natsServerRunner;

    Connection nc;

    @AfterEach
    public void after() throws Exception {
        if (nc != null)
            nc.close();

        if (natsServerRunner != null)
            natsServerRunner.close();
    }

    int port;

    @BeforeEach
    public void setup() throws Exception {
        natsServerRunner = NatsServerRunner.builder().jetstream().build();
        Thread.sleep(1);


        port = natsServerRunner.getPort();


        Options.Builder builder = new Options.Builder().connectionTimeout(Duration.ofSeconds(5))
                .servers(new String[]{"localhost:" + port});
        nc = Nats.connect(builder.build());
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

    }

    @Test
    public void testSubJetStream() throws InterruptedException {

        final NatsClient natsClient = getNatsClient();
        final NatsStream natsStream = getJetStream(natsClient);

        testJetStreamPub(natsClient, natsStream, 100_000, 10000, 10);

    }

    @Test
    public void testSubJetStream1000() throws InterruptedException {

        final NatsClient natsClient = getNatsClient();
        final NatsStream natsStream = getJetStream(natsClient);

        testJetStreamPub(natsClient, natsStream, 1000, 200, 3);

    }

    @Test
    public void testSubJetStream2000() throws InterruptedException {

        final NatsClient natsClient = getNatsClient();
        final NatsStream natsStream = getJetStream(natsClient);

        testJetStreamPub(natsClient, natsStream, 2_000, 1000, 3);

    }


    @Test
    public void testSubJetStream4000() throws InterruptedException {

        final NatsClient natsClient = getNatsClient();
        final NatsStream natsStream = getJetStream(natsClient);

        testJetStreamPub(natsClient, natsStream, 2_000, 1000, 3);

    }


    @Test
    public void testSubJetStream8000() throws InterruptedException {

        final NatsClient natsClient = getNatsClient();
        final NatsStream natsStream = getJetStream(natsClient);

        testJetStreamPub(natsClient, natsStream, 8_000, 1000, 3);

    }

    @Test
    public void testSubJetStream16000() throws InterruptedException {

        final NatsClient natsClient = getNatsClient();
        final NatsStream natsStream = getJetStream(natsClient);

        testJetStreamPub(natsClient, natsStream, 16_000, 1000, 3);

    }
    private void testJetStreamPub(NatsClient natsClient, NatsStream natsStream, final int recordCount,
    final int timeoutMS, final long timeout) throws InterruptedException {


        final CountDownLatch latch = new CountDownLatch(recordCount);
        final String data = "data";

        natsStream.subscribe(SUBJECT_NAME, event -> {

            latch.countDown();
        }, true, new PushSubscribeOptions.Builder().build());

        final long startTime = System.currentTimeMillis();

        for (int i = 0; i < recordCount; i++) {
            nc.publish(SUBJECT_NAME, (data + i).getBytes());
        }

        latch.await(timeout, TimeUnit.SECONDS);

        final long endTime = System.currentTimeMillis();

        assertEquals(0, latch.getCount());



        System.out.printf("wrote record count %d in time ms %d \n", recordCount, (endTime - startTime) );

        assertTrue((endTime - startTime) < timeoutMS);
        closeClient(natsClient);
    }


    private NatsStream getJetStream(NatsClient natsClient) throws InterruptedException {
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


    private void closeClient(NatsClient natsClient) throws InterruptedException {
        final CountDownLatch endLatch = new CountDownLatch(1);
        natsClient.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    private NatsClient getNatsClient() throws InterruptedException {
        final NatsOptions natsOptions = new NatsOptions();
        natsOptions.setVertx(Vertx.vertx());
        natsOptions.setNatsBuilder(new Options.Builder());
        natsOptions.getNatsBuilder().servers(new String[]{"localhost:" + port}).connectionListener(new ConnectionListener() {
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
