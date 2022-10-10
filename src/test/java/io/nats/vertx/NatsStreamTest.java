package io.nats.vertx;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.NatsMessage;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import nats.io.NatsServerRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class NatsStreamTest {

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
        natsServerRunner = new NatsServerRunner(0, false, true);
        Thread.sleep(1);


        port = natsServerRunner.getPort();


        Options.Builder builder = new Options.Builder()
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
    public void testSubJetStreamWithOptions() throws InterruptedException {

        final NatsClient natsClient = getNatsClient();
        final NatsStream natsStream = getJetStreamWithOptions(natsClient);

        testJetStreamPub(natsClient, natsStream);
    }

    @Test
    public void testSubJetStream() throws InterruptedException {

        final NatsClient natsClient = getNatsClient();
        final NatsStream natsStream = getJetStream(natsClient);

        testJetStreamPub(natsClient, natsStream);

    }

    private void testJetStreamPub(NatsClient natsClient, NatsStream natsStream) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        natsStream.subscribe(SUBJECT_NAME, event -> {
            queue.add(event);
            latch.countDown();
        }, true, new PushSubscribeOptions.Builder().build());

        for (int i = 0; i < 10; i++) {
            nc.publish(SUBJECT_NAME, (data + i).getBytes());
        }

        latch.await(1, TimeUnit.SECONDS);

        assertEquals(10, queue.size());

        closeClient(natsClient);
    }

    @Test
    public void testSubJetStreamWithQueueName() throws InterruptedException {

        final NatsClient natsClient = getNatsClient();
        final NatsStream natsStream = getJetStream(natsClient);

        final CountDownLatch latch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        natsStream.subscribe(SUBJECT_NAME, "FOO", event -> {
            queue.add(event);
            latch.countDown();
        }, true, new PushSubscribeOptions.Builder().build());

        for (int i = 0; i < 10; i++) {
            nc.publish(SUBJECT_NAME, (data + i).getBytes());
        }

        latch.await(1, TimeUnit.SECONDS);

        assertEquals(10, queue.size());

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


    private NatsStream getJetStreamWithOptions(NatsClient natsClient) throws InterruptedException {
        final JetStreamOptions options = JetStreamOptions.builder().build();
        final Future<NatsStream> connect = natsClient.jetStream(options);
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicReference<NatsStream> stream = new AtomicReference<>();
        connect.onSuccess(event -> {
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


    @Test
    public void testPubMessageSub() throws InterruptedException {

        final NatsClient clientPub = getNatsClient();
        final NatsClient clientSub = getNatsClient();

        final NatsStream jetStreamPub = getJetStream(clientPub);
        final NatsStream jetStreamSub = getJetStream(clientSub);

        final CountDownLatch receiveLatch = new CountDownLatch(10);
        final CountDownLatch sendLatch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        jetStreamSub.subscribe(SUBJECT_NAME, event -> {
            queue.add(event);
            receiveLatch.countDown();
        }, true, PushSubscribeOptions.builder().build());

        for (int i = 0; i < 10; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            jetStreamPub.publish(message).onSuccess(event -> sendLatch.countDown());
        }
        sendLatch.await(1, TimeUnit.SECONDS);
        receiveLatch.await(1, TimeUnit.SECONDS);

        assertEquals(10, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        clientPub.end().onSuccess(event -> endLatch.countDown());
        clientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    @Test
    public void testPubMessageSub100() throws InterruptedException {

        final NatsClient clientPub = getNatsClient();
        final NatsClient clientSub = getNatsClient();

        final NatsStream jetStreamPub = getJetStream(clientPub);
        final NatsStream jetStreamSub = getJetStream(clientSub);

        final CountDownLatch receiveLatch = new CountDownLatch(100);
        final CountDownLatch sendLatch = new CountDownLatch(100);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(200);
        final String data = "data";

        jetStreamSub.subscribe(SUBJECT_NAME, event -> {
            queue.add(event);
            receiveLatch.countDown();
        }, true, PushSubscribeOptions.builder().build());

        for (int i = 0; i < 100; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            jetStreamPub.publish(message).onSuccess(event -> sendLatch.countDown());
        }
        sendLatch.await(1, TimeUnit.SECONDS);
        receiveLatch.await(1, TimeUnit.SECONDS);

        assertEquals(100, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        clientPub.end().onSuccess(event -> endLatch.countDown());
        clientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    @Test
    public void testSubWithError() throws InterruptedException {

        final NatsClient natsClient = getNatsClient();
        final NatsStream stream = getJetStream(natsClient);


        final CountDownLatch latch = new CountDownLatch(10);
        final String data = "data";

        natsClient.exceptionHandler(event -> latch.countDown());

        stream.subscribe(SUBJECT_NAME,  event -> {
            throw new IllegalStateException("TEST SUB WITH ERROR");
        }, true, PushSubscribeOptions.builder().build());

        for (int i = 0; i < 10; i++) {
            nc.publish(SUBJECT_NAME, (data + i).getBytes());
        }

        latch.await(1, TimeUnit.SECONDS);

        closeClient(natsClient);
    }

    @Test
    public void testWriteSub() throws InterruptedException {

        final NatsClient clientPub = getNatsClient();
        final NatsClient clientSub = getNatsClient();

        final NatsStream jetStreamPub = getJetStream(clientPub);
        final NatsStream jetStreamSub = getJetStream(clientSub);

        final CountDownLatch receiveLatch = new CountDownLatch(10);
        final CountDownLatch sendLatch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        jetStreamSub.subscribe(SUBJECT_NAME, event -> {
            queue.add(event);
            receiveLatch.countDown();
        }, true, PushSubscribeOptions.builder().build());

        for (int i = 0; i < 10; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            jetStreamPub.write(message).onSuccess(event -> sendLatch.countDown());
        }
        sendLatch.await(1, TimeUnit.SECONDS);
        receiveLatch.await(1, TimeUnit.SECONDS);

        assertEquals(10, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        clientPub.end().onSuccess(event -> endLatch.countDown());
        clientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    @Test
    public void testWriteSubUnSub() throws InterruptedException {

        final NatsClient clientPub = getNatsClient();
        final NatsClient clientSub = getNatsClient();

        final NatsStream jetStreamPub = getJetStream(clientPub);
        final NatsStream jetStreamSub = getJetStream(clientSub);

        final CountDownLatch receiveLatch = new CountDownLatch(5);
        final CountDownLatch sendLatch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        jetStreamSub.subscribe(SUBJECT_NAME, event -> {
            queue.add(event);
            try {
                receiveLatch.countDown();
            } catch (Exception ex) {}
        }, true, PushSubscribeOptions.builder().build());

        for (int i = 0; i < 5; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            Thread.sleep(100);
            jetStreamPub.write(message).onSuccess(event -> sendLatch.countDown());
        }
        sendLatch.await(1, TimeUnit.SECONDS);
        receiveLatch.await(1, TimeUnit.SECONDS);

        final CountDownLatch unsubscribeLatch = new CountDownLatch(1);
        jetStreamSub.unsubscribe(SUBJECT_NAME).onSuccess(event -> {
            unsubscribeLatch.countDown();
        });

        unsubscribeLatch.await(1, TimeUnit.SECONDS);

        for (int i = 0; i < 10; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            Thread.sleep(100);
            jetStreamPub.write(message);
        }

        assertEquals(5, queue.size());

        final CountDownLatch unsubscribeLatch2 = new CountDownLatch(2);
        jetStreamSub.unsubscribe(SUBJECT_NAME + "FOO").onFailure(event -> {
            unsubscribeLatch2.countDown();
        });
        jetStreamSub.unsubscribe(SUBJECT_NAME ).onFailure(event -> {
            unsubscribeLatch2.countDown();
        });
        unsubscribeLatch2.await(1, TimeUnit.SECONDS);

        final CountDownLatch endLatch = new CountDownLatch(2);
        clientPub.end().onSuccess(event -> endLatch.countDown());
        clientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    @Test
    public void testPubAsyncResultSub() throws InterruptedException {

        final NatsClient clientPub = getNatsClient();
        final NatsClient clientSub = getNatsClient();

        final NatsStream jetStreamPub = getJetStream(clientPub);
        final NatsStream jetStreamSub = getJetStream(clientSub);

        final CountDownLatch receiveLatch = new CountDownLatch(10);
        final CountDownLatch sendLatch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        jetStreamSub.subscribe(SUBJECT_NAME, event -> {
            queue.add(event);
            receiveLatch.countDown();
        }, true, PushSubscribeOptions.builder().build());

        for (int i = 0; i < 10; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            jetStreamPub.publish(message, event -> {
                if (event.succeeded()) {
                    sendLatch.countDown();
                }
            });
        }
        sendLatch.await(1, TimeUnit.SECONDS);
        receiveLatch.await(1, TimeUnit.SECONDS);

        assertEquals(10, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        clientPub.end().onSuccess(event -> endLatch.countDown());
        clientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }



    @Test
    public void testWriteAsyncResultSub() throws InterruptedException {

        final NatsClient clientPub = getNatsClient();
        final NatsClient clientSub = getNatsClient();

        final NatsStream jetStreamPub = getJetStream(clientPub);
        final NatsStream jetStreamSub = getJetStream(clientSub);

        final CountDownLatch receiveLatch = new CountDownLatch(10);
        final CountDownLatch sendLatch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        jetStreamSub.subscribe(SUBJECT_NAME, event -> {
            queue.add(event);
            receiveLatch.countDown();
        }, true, PushSubscribeOptions.builder().build());

        for (int i = 0; i < 10; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            jetStreamPub.write(message, event -> {
                if (event.succeeded()) {
                    sendLatch.countDown();
                }
            });
        }
        sendLatch.await(1, TimeUnit.SECONDS);
        receiveLatch.await(1, TimeUnit.SECONDS);

        assertEquals(10, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        clientPub.end().onSuccess(event -> endLatch.countDown());
        clientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    @Test
    public void testPubMessageOptionsSub() throws InterruptedException {

        final NatsClient clientPub = getNatsClient();
        final NatsClient clientSub = getNatsClient();

        final NatsStream jetStreamPub = getJetStream(clientPub);
        final NatsStream jetStreamSub = getJetStream(clientSub);

        final CountDownLatch receiveLatch = new CountDownLatch(10);
        final CountDownLatch sendLatch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        jetStreamSub.subscribe(SUBJECT_NAME, event -> {
            queue.add(event);
            receiveLatch.countDown();
        }, true, PushSubscribeOptions.builder().build());

        for (int i = 0; i < 10; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            jetStreamPub.publish(message, PublishOptions.builder().build()).onSuccess(event -> sendLatch.countDown());
        }
        sendLatch.await(1, TimeUnit.SECONDS);
        receiveLatch.await(1, TimeUnit.SECONDS);

        assertEquals(10, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        clientPub.end().onSuccess(event -> endLatch.countDown());
        clientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    @Test
    public void testPubSub() throws InterruptedException {

        final NatsClient clientPub = getNatsClient();
        final NatsClient clientSub = getNatsClient();

        final NatsStream jetStreamPub = getJetStream(clientPub);
        final NatsStream jetStreamSub = getJetStream(clientSub);

        final CountDownLatch receiveLatch = new CountDownLatch(10);
        final CountDownLatch sendLatch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        jetStreamSub.subscribe(SUBJECT_NAME, event -> {
            queue.add(event);
            receiveLatch.countDown();
        }, true, PushSubscribeOptions.builder().build());

        for (int i = 0; i < 10; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            jetStreamPub.publish(SUBJECT_NAME, data+i).onSuccess(event -> sendLatch.countDown());
        }
        sendLatch.await(1, TimeUnit.SECONDS);
        receiveLatch.await(1, TimeUnit.SECONDS);

        assertEquals(10, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        clientPub.end().onSuccess(event -> endLatch.countDown());
        clientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }



    @Test
    public void testPubBytesSub() throws InterruptedException {

        final NatsClient clientPub = getNatsClient();
        final NatsClient clientSub = getNatsClient();

        final NatsStream jetStreamPub = getJetStream(clientPub);
        final NatsStream jetStreamSub = getJetStream(clientSub);

        final CountDownLatch receiveLatch = new CountDownLatch(10);
        final CountDownLatch sendLatch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        jetStreamSub.subscribe(SUBJECT_NAME, event -> {
            queue.add(event);
            receiveLatch.countDown();
        }, true, PushSubscribeOptions.builder().build());

        for (int i = 0; i < 10; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            jetStreamPub.publish(SUBJECT_NAME, (data+i).getBytes(StandardCharsets.UTF_8))
                    .onSuccess(event -> sendLatch.countDown());
        }
        sendLatch.await(1, TimeUnit.SECONDS);
        receiveLatch.await(1, TimeUnit.SECONDS);

        assertEquals(10, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        clientPub.end().onSuccess(event -> endLatch.countDown());
        clientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }

}
