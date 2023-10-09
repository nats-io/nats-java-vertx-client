package io.nats.vertx;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import nats.io.NatsServerRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
public class NatsClientTest {

    final String SUBJECT_NAME = "testSubject";
    final String DURABLE_CONSUMER_NAME = "consumer";

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
        createServer();
        port = natsServerRunner.getPort();

        Options.Builder builder = new Options.Builder().connectionTimeout(Duration.ofSeconds(5))
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
    public void testConnect() throws InterruptedException {

        final NatsClient natsClient = TestUtils.natsClient(port, Vertx.vertx(), Throwable::printStackTrace);

        assertNotNull(natsClient.getConnection());

    }

    @Test
    public void testPubSub() throws InterruptedException {

        Vertx vertx = Vertx.vertx();

        final NatsClient natsClientPub = TestUtils.natsClient(port);
        final NatsClient natsClientSub = TestUtils.natsClient(port);


        final CountDownLatch latch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        natsClientSub.subscribe(SUBJECT_NAME, event -> {
            queue.add(event);
            latch.countDown();
        });

        for (int i = 0; i < 10; i++) {
            natsClientPub.publish(SUBJECT_NAME, (data + i));
        }

        latch.await(3, TimeUnit.SECONDS);

        assertTrue(queue.size() > 8);

        natsClientSub.unsubscribe(SUBJECT_NAME)
                .onFailure(Throwable::printStackTrace)
                .onSuccess(event -> System.out.println("Success"));


        TestUtils.closeClient(natsClientPub);
        TestUtils.closeClient(natsClientSub);


    }


    @Test
    public void testPubSub100() throws InterruptedException {

        final NatsClient natsClientPub = TestUtils.natsClient(port);;
        final NatsClient natsClientSub = TestUtils.natsClient(port);


        final CountDownLatch latch = new CountDownLatch(100);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(200);
        final String data = "data";

        natsClientSub.subscribe(SUBJECT_NAME, event -> {
            queue.add(event);
            latch.countDown();
        });

        for (int i = 0; i < 100; i++) {
            natsClientPub.publish(SUBJECT_NAME, (data + i));
        }

        latch.await(10, TimeUnit.SECONDS);

        assertTrue(queue.size() > 98);
        TestUtils.closeClient(natsClientPub);
        TestUtils.closeClient(natsClientSub);


    }

    @Test
    public void testWriteSub() throws InterruptedException {

        final NatsClient natsClientPub = TestUtils.natsClient(port);
        final NatsClient natsClientSub = TestUtils.natsClient(port);


        final CountDownLatch receiveLatch = new CountDownLatch(10);
        final CountDownLatch sendLatch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        natsClientSub.subscribe(SUBJECT_NAME, event -> {
            queue.add(event);
            receiveLatch.countDown();
        });

        for (int i = 0; i < 10; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            natsClientPub.write(message).onSuccess(event -> sendLatch.countDown());
        }
        sendLatch.await(1, TimeUnit.SECONDS);
        receiveLatch.await(1, TimeUnit.SECONDS);

        assertEquals(10, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        natsClientPub.end().onSuccess(event -> endLatch.countDown());
        natsClientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    @Test
    public void testWriteAsyncResultSub() throws InterruptedException {


        final NatsClient natsClientPub = TestUtils.natsClient(port);
        final NatsClient natsClientSub = TestUtils.natsClient(port);


        final CountDownLatch receiveLatch = new CountDownLatch(10);
        final CountDownLatch sendLatch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        natsClientSub.subscribe(SUBJECT_NAME + "testWriteAsyncResultSub", event -> {
            queue.add(event);
            receiveLatch.countDown();
        });

        for (int i = 0; i < 10; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME  + "testWriteAsyncResultSub")
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            natsClientPub.write(message, event -> {
                if (event.succeeded()) {
                    sendLatch.countDown();
                }
            });
        }
        sendLatch.await(1, TimeUnit.SECONDS);
        receiveLatch.await(1, TimeUnit.SECONDS);

        assertEquals(10, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        natsClientPub.end().onSuccess(event -> endLatch.countDown());
        natsClientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    @Test
    public void testPubMessageSub() throws InterruptedException {

        final NatsClient natsClientPub = TestUtils.natsClient(port);;
        final NatsClient natsClientSub = TestUtils.natsClient(port);


        final CountDownLatch receiveLatch = new CountDownLatch(10);
        final CountDownLatch sendLatch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        natsClientSub.subscribe(SUBJECT_NAME, event -> {
            queue.add(event);
            receiveLatch.countDown();
        });

        for (int i = 0; i < 10; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            natsClientPub.publish(message).onSuccess(event -> sendLatch.countDown());
        }
        sendLatch.await(3, TimeUnit.SECONDS);
        receiveLatch.await(3, TimeUnit.SECONDS);

        assertTrue(queue.size() >= 9);

        final CountDownLatch endLatch = new CountDownLatch(2);
        natsClientPub.end().onSuccess(event -> endLatch.countDown());
        natsClientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    @Test
    public void testPubMessageAsyncResultSub() throws InterruptedException {


        final NatsClient natsClientPub = TestUtils.natsClient(port);
        final NatsClient natsClientSub = TestUtils.natsClient(port);


        final CountDownLatch receiveLatch = new CountDownLatch(10);
        final CountDownLatch sendLatch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        natsClientSub.subscribe(SUBJECT_NAME, event -> {
            queue.add(event);
            receiveLatch.countDown();
        });

        for (int i = 0; i < 10; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            natsClientPub.publish(message, event -> {
                if (event.succeeded()) {
                    sendLatch.countDown();
                }
            });
        }
        sendLatch.await(1, TimeUnit.SECONDS);
        receiveLatch.await(1, TimeUnit.SECONDS);

        assertEquals(10, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        natsClientPub.end().onSuccess(event -> endLatch.countDown());
        natsClientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }

    @Test
    public void testPubMessageSubWithHeaderAndReplyTo() throws InterruptedException {


        final NatsClient natsClientPub = TestUtils.natsClient(port);
        final NatsClient natsClientSub = TestUtils.natsClient(port);


        final CountDownLatch receiveLatch = new CountDownLatch(10);
        final CountDownLatch sendLatch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        final Headers headers = new Headers();
        headers.add("header1", "value1");

        natsClientSub.subscribe(SUBJECT_NAME, event -> {
            if (event.hasHeaders() && event.getReplyTo().equals("reply-to")) {
                queue.add(event);
                receiveLatch.countDown();
            }
        });

        for (int i = 0; i < 10; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            natsClientPub.publish(SUBJECT_NAME, "reply-to", headers,
                    data.getBytes(StandardCharsets.UTF_8)).onSuccess(event -> sendLatch.countDown());
        }
        sendLatch.await(1, TimeUnit.SECONDS);
        receiveLatch.await(1, TimeUnit.SECONDS);

        assertEquals(10, queue.size());

        TestUtils.closeClient(natsClientPub);
        TestUtils.closeClient(natsClientSub);
    }


    @Test
    public void testPubMessageSubWithHeader() throws InterruptedException {


        final NatsClient natsClientPub = TestUtils.natsClient(port);
        final NatsClient natsClientSub = TestUtils.natsClient(port);


        final CountDownLatch receiveLatch = new CountDownLatch(10);
        final CountDownLatch sendLatch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        final Headers headers = new Headers();
        headers.add("header1", "value1");

        natsClientSub.subscribe(SUBJECT_NAME, event -> {
            if (event.hasHeaders()) {
                queue.add(event);
                receiveLatch.countDown();
            }
        });

        for (int i = 0; i < 10; i++) {

            final NatsMessage message = NatsMessage.builder().subject(SUBJECT_NAME)
                    .data(data + i, StandardCharsets.UTF_8)
                    .build();
            natsClientPub.publish(SUBJECT_NAME,  headers,
                    data.getBytes(StandardCharsets.UTF_8)).onSuccess(event -> sendLatch.countDown());
        }
        sendLatch.await(1, TimeUnit.SECONDS);
        receiveLatch.await(1, TimeUnit.SECONDS);

        assertEquals(10, queue.size());

        TestUtils.closeClient(natsClientPub);
        TestUtils.closeClient(natsClientSub);
    }


    @Test
    public void testPubSubReplyTo() throws InterruptedException {

        final NatsClient natsClientPub = TestUtils.natsClient(port);
        final NatsClient natsClientSub = TestUtils.natsClient(port);


        final CountDownLatch latch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        natsClientSub.subscribe(SUBJECT_NAME, event -> {
            queue.add(event);
            latch.countDown();
        });

        for (int i = 0; i < 10; i++) {
            natsClientPub.publish(SUBJECT_NAME, "replyTo", (data + i));
        }

        latch.await(1, TimeUnit.SECONDS);

        assertEquals(10, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        natsClientPub.end().onSuccess(event -> endLatch.countDown());
        natsClientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    @Test
    public void testRequestReply() throws InterruptedException {

        final NatsClient natsRequester = TestUtils.natsClient(port);
        final NatsClient natsReply = TestUtils.natsClient(port);


        natsReply.subscribe("REQUEST_SUBJECT", event -> {
            natsReply.publish(event.getReplyTo(), event.getData());
        });

        final Future<Message> request = natsRequester.request("REQUEST_SUBJECT", "HELLO_MOM");


        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Message> message = new AtomicReference<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        request.onSuccess(event -> {
            message.set(event);
            latch.countDown();

        }).onFailure(event -> {
            error.set(event);
            latch.countDown();
        });

        latch.await(3, TimeUnit.SECONDS);

        if (error.get() != null) {
            fail();
        }

        final Message reply = message.get();
        assertNotNull(reply);
        assertEquals("HELLO_MOM", new String(reply.getData(), StandardCharsets.UTF_8));

        TestUtils.closeClient(natsRequester);
        TestUtils.closeClient(natsReply);
    }

    @Test
    public void testRequestMessageReply() throws InterruptedException {

        final NatsClient natsRequester = TestUtils.natsClient(port);
        final NatsClient natsReply = TestUtils.natsClient(port);


        natsReply.subscribe("REQUEST_SUBJECT", event -> {
            natsReply.publish(event.getReplyTo(), event.getData());
        });


        final Message requestMessage = NatsMessage.builder()
                .subject("REQUEST_SUBJECT")
                .data("HELLO_MOM", StandardCharsets.UTF_8)
                .build();
        final Future<Message> request = natsRequester
                .request(requestMessage);


        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Message> message = new AtomicReference<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        request.onSuccess(event -> {
            message.set(event);
            latch.countDown();

        }).onFailure(event -> {
            error.set(event);
            latch.countDown();
        });

        latch.await(3, TimeUnit.SECONDS);

        if (error.get() != null) {
            fail();
        }

        final Message reply = message.get();
        assertNotNull(reply);
        assertEquals("HELLO_MOM", new String(reply.getData(), StandardCharsets.UTF_8));

        TestUtils.closeClient(natsRequester);
        TestUtils.closeClient(natsReply);
    }


    @Test
    public void testRequestMessageReplyWithHeaders() throws InterruptedException {

        final NatsClient natsRequester = TestUtils.natsClient(port);
        final NatsClient natsReply = TestUtils.natsClient(port);
        final Headers headers = new Headers().put("key", "value");


        natsReply.subscribe("REQUEST_SUBJECT", event -> {
            if (event.hasHeaders()) {
                if (event.getHeaders().containsKey("key")) {
                    natsReply.publish(event.getReplyTo(), event.getData());
                }
            }
        });


        final Message requestMessage = NatsMessage.builder()
                .subject("REQUEST_SUBJECT")
                .data("HELLO_MOM", StandardCharsets.UTF_8)
                .build();
        final Future<Message> request = natsRequester
                .request(requestMessage.getSubject(), headers, requestMessage.getData());


        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Message> message = new AtomicReference<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        request.onSuccess(event -> {
            message.set(event);
            latch.countDown();

        }).onFailure(event -> {
            error.set(event);
            latch.countDown();
        });

        latch.await(3, TimeUnit.SECONDS);

        if (error.get() != null) {
            fail();
        }

        final Message reply = message.get();
        assertNotNull(reply);
        assertEquals("HELLO_MOM", new String(reply.getData(), StandardCharsets.UTF_8));



        TestUtils.closeClient(natsRequester);
        TestUtils.closeClient(natsReply);
    }


    @Test
    public void testRequestMessageReplyWithHeadersAndDuration() throws InterruptedException {

        final NatsClient natsRequester = TestUtils.natsClient(port);
        final NatsClient natsReply = TestUtils.natsClient(port);
        final Headers headers = new Headers().put("key", "value");
        final Duration duration = Duration.ofSeconds(5);


        natsReply.subscribe("REQUEST_SUBJECT", event -> {
            if (event.hasHeaders()) {
                if (event.getHeaders().containsKey("key")) {
                    natsReply.publish(event.getReplyTo(), event.getData());
                }
            }
        });


        final Message requestMessage = NatsMessage.builder()
                .subject("REQUEST_SUBJECT")
                .data("HELLO_MOM", StandardCharsets.UTF_8)
                .build();
        final Future<Message> request = natsRequester
                .requestWithTimeout(requestMessage.getSubject(), headers, requestMessage.getData(), duration);


        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Message> message = new AtomicReference<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        request.onSuccess(event -> {
            message.set(event);
            latch.countDown();

        }).onFailure(event -> {
            error.set(event);
            latch.countDown();
        });

        latch.await(3, TimeUnit.SECONDS);

        if (error.get() != null) {
            fail();
        }

        final Message reply = message.get();
        assertNotNull(reply);
        assertEquals("HELLO_MOM", new String(reply.getData(), StandardCharsets.UTF_8));



        TestUtils.closeClient(natsRequester);
        TestUtils.closeClient(natsReply);
    }

    @Test
    public void testRequestMessageReplyAsyncResult() throws InterruptedException {

        final NatsClient natsRequester = TestUtils.natsClient(port);
        final NatsClient natsReply = TestUtils.natsClient(port);


        natsReply.subscribe("REQUEST_SUBJECT", event -> {
            natsReply.publish(event.getReplyTo(), event.getData());
        });


        final Message requestMessage = NatsMessage.builder()
                .subject("REQUEST_SUBJECT")
                .data("HELLO_MOM", StandardCharsets.UTF_8)
                .build();



        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Message> message = new AtomicReference<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final Handler<AsyncResult<Message>> handler = event -> {
            if (event.succeeded()) {
                message.set(event.result());
                latch.countDown();
            } else {
                error.set(event.cause());
                latch.countDown();
            }
        };

        natsRequester.request(requestMessage, handler);


        latch.await(3, TimeUnit.SECONDS);

        if (error.get() != null) {
            fail();
        }

        final Message reply = message.get();
        assertNotNull(reply);
        assertEquals("HELLO_MOM", new String(reply.getData(), StandardCharsets.UTF_8));

        TestUtils.closeClient(natsRequester);
        TestUtils.closeClient(natsReply);
    }

    @Test
    public void testSub() throws InterruptedException {

        final NatsClient natsClient = TestUtils.natsClient(port);


        final CountDownLatch latch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        natsClient.subscribe(SUBJECT_NAME + "testSub", event -> {
            queue.add(event);
            latch.countDown();
        });

        for (int i = 0; i < 10; i++) {
            nc.publish(SUBJECT_NAME + "testSub", (data + i).getBytes());
            try {
                nc.flush(Duration.ofMillis(500));
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
        }

        latch.await(10, TimeUnit.SECONDS);

        assertTrue( queue.size() >= 8);

        TestUtils.closeClient(natsClient);
    }

    @Test
    public void testSubWithError() throws InterruptedException {

        final NatsClient natsClient = TestUtils.natsClient(port);


        final CountDownLatch latch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        natsClient.exceptionHandler(event -> latch.countDown());

        natsClient.subscribe(SUBJECT_NAME, event -> {
            throw new IllegalStateException("TEST SUB WITH ERROR");
        });

        for (int i = 0; i < 10; i++) {
            nc.publish(SUBJECT_NAME, (data + i).getBytes());
        }

        latch.await(1, TimeUnit.SECONDS);

        TestUtils.closeClient(natsClient);
    }


    @Test
    public void testSubWithQueueName() throws InterruptedException {

        final NatsClient natsClient = TestUtils.natsClient(port);


        final CountDownLatch latch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        natsClient.subscribe(SUBJECT_NAME, "FOO", event -> {
            queue.add(event);
            latch.countDown();
        });

        for (int i = 0; i < 10; i++) {
            nc.publish(SUBJECT_NAME, (data + i).getBytes());
            try {
                nc.flush(Duration.ofSeconds(1));
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
        }

        latch.await(10, TimeUnit.SECONDS);

        natsClient.unsubscribe(SUBJECT_NAME);

        assertTrue(queue.size() > 8);
        TestUtils.closeClient(natsClient);
    }

    @Test
    public void testForceFail() throws InterruptedException {

        try {
            final NatsClient natsClient = TestUtils.natsClient(port + 10);
            fail();
        } catch (Exception ex) {

        }
    }




    private void createServer() throws Exception {
        natsServerRunner = TestUtils.startServer();
        port = natsServerRunner.getPort();
    }
}
