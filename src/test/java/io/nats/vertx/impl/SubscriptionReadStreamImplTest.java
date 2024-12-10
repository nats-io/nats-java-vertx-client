package io.nats.vertx.impl;


import io.nats.NatsServerRunner;
import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.vertx.*;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@ExtendWith(VertxExtension.class)
class SubscriptionReadStreamImplTest {


    final String SUBJECT_NAME = "subscriptionTest";
    String subjectName;
    int subjectIndex = 0;
    static NatsServerRunner natsServerRunner;
    Connection nc;
    static int port;

    @Test
    public void testFetch(final VertxTestContext testContext) {
        final NatsClient natsClient = TestUtils.natsClient(port, Vertx.vertx(), Throwable::printStackTrace);
        final NatsStream natsStream = TestUtils.jetStream(natsClient);
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
                                TestUtils.closeClient(natsClient);
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
    public void testIterate(final VertxTestContext testContext) {
        final NatsClient natsClient = TestUtils.natsClient(port, Vertx.vertx(), Throwable::printStackTrace);
        final NatsStream natsStream = TestUtils.jetStream(natsClient);
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
                                TestUtils.closeClient(natsClient);
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
           jsm.addStream(sc);
        }

    }
    @BeforeAll
    public static void setupAll() throws Exception {
        natsServerRunner = TestUtils.startServer();
        port = natsServerRunner.getPort();
    }






}
