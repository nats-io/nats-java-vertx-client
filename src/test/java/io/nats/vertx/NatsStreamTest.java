package io.nats.vertx;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import nats.io.NatsServerRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
            //ex.printStackTrace();
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
            queue.add(event.message());
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
            queue.add(event.message());
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
    public void testSubJetStreamWithPullDebug() throws Exception {

        final NatsClient natsClient = getNatsClient();
        final NatsStream natsStream = getJetStream(natsClient);

        final CountDownLatch latch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        final JetStreamSubscription subscription = nc.jetStream().subscribe(SUBJECT_NAME);
//
//        natsStream.subscribe(SUBJECT_NAME, event -> {

//        }, new PullSubscribeOptions.Builder().name("bob").durable("bob").stream(SUBJECT_NAME).build());

        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        executorService.submit(() -> {
            Message message = null;
            try {
                while (true) {
                    message = subscription.nextMessage(Duration.ofMillis(100));
                    message.ack();
                    queue.add(message);
                    latch.countDown();
                    if (queue.size() == 10) break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        for (int i = 0; i < 10; i++) {
            nc.publish(SUBJECT_NAME, (data + i).getBytes());
        }

        latch.await(1, TimeUnit.SECONDS);

        assertEquals(10, queue.size());

        executorService.awaitTermination(1, TimeUnit.SECONDS);
        closeClient(natsClient);
    }


    @Test
    public void testSubJetStreamWithPull() throws Exception {

        final NatsClient natsClient = getNatsClient();
        final NatsStream natsStream = getJetStream(natsClient);

        final CountDownLatch latch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        natsStream.subscribe(SUBJECT_NAME, event -> {
            event.message().ack();
            queue.add(event.message());
            latch.countDown();
        });


        for (int i = 0; i < 10; i++) {
            nc.publish(SUBJECT_NAME, (data + i).getBytes());
        }

        latch.await(1, TimeUnit.SECONDS);

        natsStream.unsubscribe(SUBJECT_NAME);
        Thread.sleep(1000);


        assertEquals(10, queue.size());

        closeClient(natsClient);
    }


    @Test
    public void testSubJetStreamWithPullOptions() throws Exception {

        final NatsClient natsClient = getNatsClient();
        final NatsStream natsStream = getJetStream(natsClient);

        final CountDownLatch latch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";
        final PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                .durable("durable-name-is-required")
                .build();

        natsStream.subscribe(SUBJECT_NAME, event -> {
            event.message().ack();
            queue.add(event.message());
            latch.countDown();
        }, pullOptions);


        for (int i = 0; i < 10; i++) {
            nc.publish(SUBJECT_NAME, (data + i).getBytes());
        }

        latch.await(1, TimeUnit.SECONDS);

        natsStream.unsubscribe(SUBJECT_NAME);
        Thread.sleep(1000);


        assertEquals(10, queue.size());

        closeClient(natsClient);
    }


    @Test
    public void testSubJetStreamWithPullBatch() throws Exception {

        final NatsClient natsClient = getNatsClient();
        final NatsStream natsStream = getJetStream(natsClient);

        final CountDownLatch latch = new CountDownLatch(50);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(55);
        final String data = "data";

        final PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                .durable("durable-name-is-required")
                .build();

        natsStream.subscribeBatch(SUBJECT_NAME, messages -> {
            messages.forEach(event -> {
                        queue.add(event.message());
                        latch.countDown();
                    }
            );
        }, 10, Duration.ofMillis(10), pullOptions);


        for (int i = 0; i < 50; i++) {
            nc.publish(SUBJECT_NAME, (data + i).getBytes());
        }

        latch.await(1, TimeUnit.SECONDS);

        natsStream.unsubscribe(SUBJECT_NAME);
        Thread.sleep(1000);


        assertEquals(50, queue.size());

        closeClient(natsClient);
    }



    @Test
    public void testSubJetStreamWithBatch() throws Exception {

        final NatsClient natsClient = getNatsClient();
        final NatsStream natsStream = getJetStream(natsClient);

        final CountDownLatch latch = new CountDownLatch(50);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(55);
        final String data = "data";

        final PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                .durable("durable-name-is-required")
                .build();

        natsStream.subscribeWithBatch(SUBJECT_NAME, event -> {
                        queue.add(event.message());
                        latch.countDown();
        }, 10, Duration.ofMillis(10), pullOptions);


        for (int i = 0; i < 50; i++) {
            nc.publish(SUBJECT_NAME, (data + i).getBytes());
        }

        latch.await(1, TimeUnit.SECONDS);

        natsStream.unsubscribe(SUBJECT_NAME);
        Thread.sleep(1000);


        assertEquals(50, queue.size());

        closeClient(natsClient);
    }

    private NatsStream getJetStream(NatsClient natsClient) throws InterruptedException {
        final Future<NatsStream> connect = natsClient.jetStream();


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

    private NatsClient getNatsClient(Handler<Throwable> exceptionHandler) throws InterruptedException {
        final NatsOptions natsOptions = new NatsOptions();
        natsOptions.setVertx(Vertx.vertx());
        natsOptions.setExceptionHandler(exceptionHandler);
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
    private NatsClient getNatsClient() throws InterruptedException {
            return getNatsClient(event -> event.printStackTrace());
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
            queue.add(event.message());
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


        jetStreamSub.unsubscribe(SUBJECT_NAME).onSuccess(event -> System.out.println("Unsubscribed"))
                .onFailure(Throwable::printStackTrace);

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
            queue.add(event.message());
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
            queue.add(event.message());
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
            queue.add(event.message());
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
            queue.add(event.message());
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
            queue.add(event.message());
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
            queue.add(event.message());
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
    public void testPubMessageOptionsSubWithHeaders() throws InterruptedException {

        final NatsClient clientPub = getNatsClient();
        final NatsClient clientSub = getNatsClient();
        final Headers headers = new Headers().put("foo", "bar");

        final NatsStream jetStreamPub = getJetStream(clientPub);
        final NatsStream jetStreamSub = getJetStream(clientSub);

        final CountDownLatch receiveLatch = new CountDownLatch(10);
        final CountDownLatch sendLatch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        jetStreamSub.subscribe(SUBJECT_NAME, event -> {

            if (event.hasHeaders()) {
                assertEquals("bar", event.getHeaders().get("foo").get(0));
                queue.add(event.message());
                receiveLatch.countDown();
            }

        }, true, PushSubscribeOptions.builder().build());

        for (int i = 0; i < 10; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            jetStreamPub.publish(message.getSubject(), headers, message.getData()).onSuccess(event -> sendLatch.countDown());
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
    public void testPubMessageOptionsSubWithHeadersAndPubOptions() throws InterruptedException {

        final NatsClient clientPub = getNatsClient();
        final NatsClient clientSub = getNatsClient();
        final Headers headers = new Headers().put("foo", "bar");

        final NatsStream jetStreamPub = getJetStream(clientPub);
        final NatsStream jetStreamSub = getJetStream(clientSub);

        final CountDownLatch receiveLatch = new CountDownLatch(10);
        final CountDownLatch sendLatch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        jetStreamSub.subscribe(SUBJECT_NAME, event -> {

            if (event.hasHeaders()) {
                assertEquals("bar", event.getHeaders().get("foo").get(0));
                queue.add(event.message());
                receiveLatch.countDown();
            }

        }, true, PushSubscribeOptions.builder().build());

        for (int i = 0; i < 10; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            jetStreamPub.publish(message.getSubject(), headers, message.getData(),
                    PublishOptions.builder().build()).onSuccess(event -> sendLatch.countDown());
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
            queue.add(event.message());
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
    public void testPubSubFailsAndFutureGetsCalled() throws InterruptedException {

        final AtomicInteger sends = new AtomicInteger();
        final AtomicInteger errors = new AtomicInteger();
        final AtomicInteger errorsFromHandler = new AtomicInteger();

        final NatsClient clientPub = getNatsClient(event -> {
            errorsFromHandler.incrementAndGet();
        } );
        final NatsClient clientSub = getNatsClient();

        final NatsStream jetStreamPub = getJetStream(clientPub);
        final NatsStream jetStreamSub = getJetStream(clientSub);

        final CountDownLatch receiveLatch = new CountDownLatch(5);
        final CountDownLatch errorsLatch = new CountDownLatch(5);


        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        jetStreamSub.subscribe(SUBJECT_NAME, event -> {
            queue.add(event.message());
            receiveLatch.countDown();
        }, true, PushSubscribeOptions.builder().build());

        for (int i = 0; i < 10; i++) {


            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();

            jetStreamPub.publish(SUBJECT_NAME, data + i)
                    .onSuccess(event -> {

                                sends.incrementAndGet();
                                System.out.println("SUCCESS " + sends.get());
                            }
                    ).onFailure(error -> {
                        System.out.println("ERROR " + errors.get());
                        errors.incrementAndGet();
                        errorsLatch.countDown();
                    });


            if (i == 4) {
                Thread.sleep(1000);
                natsServerRunner.close();
                Thread.sleep(1000);
            }
        }

        Thread.sleep(200);
        receiveLatch.await(1, TimeUnit.SECONDS);
        errorsLatch.await(10, TimeUnit.SECONDS);

        assertEquals(5, queue.size());

        assertTrue(errorsFromHandler.get() >= 5);
        assertEquals(5, sends.get());
        assertEquals(5, errors.get());


        final CountDownLatch endLatch = new CountDownLatch(2);
        clientPub.end().onSuccess(event -> endLatch.countDown());
        clientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    @Test
    public void testPubSubFailsAndFutureGetsCalledWithPublishOptions() throws InterruptedException {

        final AtomicInteger sends = new AtomicInteger();
        final AtomicInteger errors = new AtomicInteger();
        final AtomicInteger errorsFromHandler = new AtomicInteger();

        final NatsClient clientPub = getNatsClient(event -> {
            errorsFromHandler.incrementAndGet();
        } );
        final NatsClient clientSub = getNatsClient();

        final NatsStream jetStreamPub = getJetStream(clientPub);
        final NatsStream jetStreamSub = getJetStream(clientSub);

        final CountDownLatch receiveLatch = new CountDownLatch(5);
        final CountDownLatch errorsLatch = new CountDownLatch(5);


        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        jetStreamSub.subscribe(SUBJECT_NAME, event -> {
            queue.add(event.message());
            receiveLatch.countDown();
        }, true, PushSubscribeOptions.builder().build());

        for (int i = 0; i < 10; i++) {

            PublishOptions po = PublishOptions.builder().build();

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();

            jetStreamPub.publish(message, po)
                    .onSuccess(event -> {

                                sends.incrementAndGet();
                                System.out.println("SUCCESS " + sends.get());
                            }
                    ).onFailure(error -> {
                        System.out.println("ERROR " + errors.get());
                        errors.incrementAndGet();
                        errorsLatch.countDown();
                    });


            if (i == 4) {
                Thread.sleep(1000);
                natsServerRunner.close();
                Thread.sleep(1000);
            }
        }

        Thread.sleep(1000);
        receiveLatch.await(10, TimeUnit.SECONDS);
        errorsLatch.await(10, TimeUnit.SECONDS);

        assertEquals(5, queue.size());

        assertTrue(errorsFromHandler.get() >= 5);
        assertEquals(5, sends.get());
        assertEquals(5, errors.get());


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
            queue.add(event.message());
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
