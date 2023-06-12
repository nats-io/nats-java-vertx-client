package io.nats.vertx.impl;


import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.support.Status;
import io.nats.vertx.*;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import nats.io.NatsServerRunner;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(VertxExtension.class)
class SubscriptionReadStreamImplTest {


    final String SUBJECT_NAME = "subscriptionTest";
    String subjectName;
    int subjectIndex = 0;
    static NatsServerRunner natsServerRunner;
    Connection nc;
    static int port;

    @Test
    public void testFetch(final VertxTestContext testContext) throws Exception {
        final NatsClient natsClient = natsClient(Vertx.vertx(), testContext, error -> {
          error.printStackTrace();
        });
        final NatsStream natsStream = jetStream(natsClient);
        final String data = "data";
        final Future<SubscriptionReadStream> subscribeFuture = natsStream.subscribe(subjectName,
                PullSubscribeOptions.builder().build());
        subscribeFuture.onSuccess(subscription -> {

                    //Now that you are subscribed, send ten messages.
                    for (int i = 0; i < 10; i++) {
                        nc.publish(subjectName, (data + i).getBytes());
                    }

                    final AtomicInteger messageCount = new AtomicInteger();
                    for (int i = 0; i < 10; i++) {
                        final Future<List<NatsVertxMessage>> messageFuture = subscription.fetch(10, Duration.ofSeconds(5));
                        messageFuture.onSuccess(events -> {
                            events.forEach(NatsVertxMessage::ack);
                            messageCount.addAndGet(events.size());
                            if (messageCount.get() > 9) {
                                testContext.completeNow();
                                closeClient(natsClient);
                            }
                        }).onFailure(testContext::failNow);
                    }
                }
        ).onFailure(event ->
                {
                    event.printStackTrace();
                    testContext.failNow(event);
                }
        );
    }



    @Test
    public void testIterate(final VertxTestContext testContext) throws Exception {
        final NatsClient natsClient = natsClient(Vertx.vertx(), testContext, error -> {
            error.printStackTrace();
        });
        final NatsStream natsStream = jetStream(natsClient);
        final String data = "data";
        final Future<SubscriptionReadStream> subscribeFuture = natsStream.subscribe(subjectName,
                PullSubscribeOptions.builder().build());
        subscribeFuture.onSuccess(subscription -> {

                    //Now that you are subscribed, send ten messages.
                    for (int i = 0; i < 10; i++) {
                        nc.publish(subjectName, (data + i).getBytes());
                    }

                    final AtomicInteger messageCount = new AtomicInteger();
                    for (int i = 0; i < 10; i++) {
                        final Future<Iterator<NatsVertxMessage>> messageFuture = subscription.iterate(10, Duration.ofSeconds(5));
                        messageFuture.onSuccess(events -> {

                            while (events.hasNext()) {
                                NatsVertxMessage e = events.next();
                                messageCount.addAndGet(1);
                                e.ack();
                            }
                            if (messageCount.get() > 9) {
                                testContext.completeNow();
                                closeClient(natsClient);
                            }
                        }).onFailure(testContext::failNow);
                    }
                }
        ).onFailure(event ->
                {
                    event.printStackTrace();
                    testContext.failNow(event);
                }
        );
    }

    @AfterAll
    public static void afterAll() throws Exception {


        if (natsServerRunner != null)
            natsServerRunner.close();
    }

    @AfterEach
    public void afterEach() throws Exception {
        if (nc != null)
            nc.close();
    }

    @BeforeEach
    public void setupEach() throws Exception {
        subjectName = SUBJECT_NAME + subjectIndex;
        subjectIndex++;

        Options.Builder builder = new Options.Builder().connectionTimeout(Duration.ofSeconds(5))
                .servers(new String[]{"localhost:" + port});
        nc = Nats.connect(builder.build());
        JetStreamManagement jsm = nc.jetStreamManagement();
        StreamInfo streamInfo = null;

        try {
            streamInfo = jsm.getStreamInfo(subjectName);
        } catch (Exception ex) {
            //ex.printStackTrace();
        }

        if (streamInfo == null) {
            StreamConfiguration sc = StreamConfiguration.builder().name(subjectName).storageType(StorageType.Memory).build();
            // Add or use an existing stream.
            StreamInfo streamInfo1 = jsm.addStream(sc);
        }

    }
    @BeforeAll
    public static void setupAll() throws Exception {
        natsServerRunner = new NatsServerRunner(0, false, true);
        port = natsServerRunner.getPort();



        for (int i = 0; i < 100; i++) {
            Thread.sleep(200);
            final AtomicReference<ConnectionListener.Events> lastEvent = new AtomicReference<>();
            final CountDownLatch latch = new CountDownLatch(1);
            final String serverName = "name" + System.currentTimeMillis();

            Options.Builder builder = new Options.Builder().connectionTimeout(Duration.ofSeconds(5))
                    .servers(new String[]{"localhost:" + port}).errorListener(new ErrorListener() {
                        @Override
                        public void errorOccurred(Connection conn, String error) {
                            System.out.println(error);
                        }

                        @Override
                        public void exceptionOccurred(Connection conn, Exception exp) {
                            exp.printStackTrace();
                        }

                        @Override
                        public void slowConsumerDetected(Connection conn, Consumer consumer) {
                            ErrorListener.super.slowConsumerDetected(conn, consumer);
                        }

                        @Override
                        public void messageDiscarded(Connection conn, Message msg) {
                            ErrorListener.super.messageDiscarded(conn, msg);
                        }

                        @Override
                        public void heartbeatAlarm(Connection conn, JetStreamSubscription sub, long lastStreamSequence, long lastConsumerSequence) {
                            ErrorListener.super.heartbeatAlarm(conn, sub, lastStreamSequence, lastConsumerSequence);
                        }

                        @Override
                        public void unhandledStatus(Connection conn, JetStreamSubscription sub, Status status) {
                            ErrorListener.super.unhandledStatus(conn, sub, status);
                        }

                        @Override
                        public void pullStatusWarning(Connection conn, JetStreamSubscription sub, Status status) {
                            ErrorListener.super.pullStatusWarning(conn, sub, status);
                        }

                        @Override
                        public void pullStatusError(Connection conn, JetStreamSubscription sub, Status status) {
                            ErrorListener.super.pullStatusError(conn, sub, status);
                        }

                        @Override
                        public void flowControlProcessed(Connection conn, JetStreamSubscription sub, String subject, FlowControlSource source) {
                            ErrorListener.super.flowControlProcessed(conn, sub, subject, source);
                        }
                    }).connectionListener(new ConnectionListener() {
                        @Override
                        public void connectionEvent(Connection conn, Events type) {
                            lastEvent.set(type);
                            latch.countDown();
                            System.out.println("CONNECTION " + type);
                        }
                    }).connectionName(serverName);
            Connection connect = Nats.connect(builder.build());
            latch.await(1, TimeUnit.SECONDS);
            if (lastEvent.get() == ConnectionListener.Events.CONNECTED) {
                connect.close();
                break;
            }

        }
    }


    private NatsClient natsClient(Vertx vertx, VertxTestContext testContext, Handler<Throwable> exceptionHandler) {
        final NatsOptions natsOptions = new NatsOptions();
        natsOptions.setVertx(vertx);
        natsOptions.setExceptionHandler(exceptionHandler);
        natsOptions.setNatsBuilder(new Options.Builder().connectionTimeout(Duration.ofSeconds(5)));
        System.out.println("Client connecting to localhost:" + port);
        natsOptions.getNatsBuilder().servers(new String[]{"localhost:" + port})
                .connectionListener((conn, type) -> {
                    System.out.println("Connection EVENT " + type);

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

    private NatsStream jetStream(NatsClient natsClient) {
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
        try {
            latch.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (error.get() != null) {
            fail();
        }
        return stream.get();
    }

    private void closeClient(NatsClient natsClient) {
        final CountDownLatch endLatch = new CountDownLatch(1);
        natsClient.end().onSuccess(event -> endLatch.countDown());
        try {
            endLatch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


}
