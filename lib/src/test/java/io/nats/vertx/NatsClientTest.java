package io.nats.vertx;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.NatsMessage;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;
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
    public void testConnect() throws InterruptedException {

        final NatsClient natsClient = getNatsClient();

        assertNotNull(natsClient.getConnection());

    }

    @Test
    public void testPubSub() throws InterruptedException {

        final NatsClient natsClientPub = getNatsClient();
        final NatsClient natsClientSub = getNatsClient();


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

        latch.await();

        assertEquals(10, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        natsClientPub.end().onSuccess(event -> endLatch.countDown());
        natsClientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }

    @Test
    public void testWriteSub() throws InterruptedException {

        final NatsClient natsClientPub = getNatsClient();
        final NatsClient natsClientSub = getNatsClient();


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
        sendLatch.await();
        receiveLatch.await();

        assertEquals(10, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        natsClientPub.end().onSuccess(event -> endLatch.countDown());
        natsClientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    @Test
    public void testWriteAsyncResultSub() throws InterruptedException {


        final NatsClient natsClientPub = getNatsClient();
        final NatsClient natsClientSub = getNatsClient();


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
            natsClientPub.write(message, new Handler<AsyncResult<Void>>() {
                @Override
                public void handle(AsyncResult<Void> event) {
                    if (event.succeeded()) {
                        sendLatch.countDown();
                    }
                }
            });
        }
        sendLatch.await();
        receiveLatch.await();

        assertEquals(10, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        natsClientPub.end().onSuccess(event -> endLatch.countDown());
        natsClientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    @Test
    public void testPubMessageSub() throws InterruptedException {

        final NatsClient natsClientPub = getNatsClient();
        final NatsClient natsClientSub = getNatsClient();


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
        sendLatch.await();
        receiveLatch.await();

        assertEquals(10, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        natsClientPub.end().onSuccess(event -> endLatch.countDown());
        natsClientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    @Test
    public void testPubMessageAsyncResultSub() throws InterruptedException {


        final NatsClient natsClientPub = getNatsClient();
        final NatsClient natsClientSub = getNatsClient();


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
        sendLatch.await();
        receiveLatch.await();

        assertEquals(10, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        natsClientPub.end().onSuccess(event -> endLatch.countDown());
        natsClientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    @Test
    public void testPubSubReplyTo() throws InterruptedException {

        final NatsClient natsClientPub = getNatsClient();
        final NatsClient natsClientSub = getNatsClient();


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

        latch.await();

        assertEquals(10, queue.size());

        final CountDownLatch endLatch = new CountDownLatch(2);
        natsClientPub.end().onSuccess(event -> endLatch.countDown());
        natsClientSub.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }


    @Test
    public void testRequestReply() throws InterruptedException {

        final NatsClient natsRequester = getNatsClient();
        final NatsClient natsReply = getNatsClient();


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

        closeClient(natsRequester);
        closeClient(natsReply);
    }

    @Test
    public void testRequestMessageReply() throws InterruptedException {

        final NatsClient natsRequester = getNatsClient();
        final NatsClient natsReply = getNatsClient();


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

        closeClient(natsRequester);
        closeClient(natsReply);
    }

    @Test
    public void testRequestMessageReplyAsyncResult() throws InterruptedException {

        final NatsClient natsRequester = getNatsClient();
        final NatsClient natsReply = getNatsClient();


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

        closeClient(natsRequester);
        closeClient(natsReply);
    }

    @Test
    public void testSub() throws InterruptedException {

        final NatsClient natsClient = getNatsClient();


        final CountDownLatch latch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        natsClient.subscribe(SUBJECT_NAME, event -> {
            queue.add(event);
            latch.countDown();
        });

        for (int i = 0; i < 10; i++) {
            nc.publish(SUBJECT_NAME, (data + i).getBytes());
        }

        latch.await();

        assertEquals(10, queue.size());

        closeClient(natsClient);
    }

    @Test
    public void testSubWithError() throws InterruptedException {

        final NatsClient natsClient = getNatsClient();


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

        latch.await();

        closeClient(natsClient);
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

        latch.await();

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

        latch.await();

        assertEquals(10, queue.size());

        closeClient(natsClient);
    }

    private NatsStream getJetStream(NatsClient natsClient) throws InterruptedException {
        Future<NatsStream> connect = natsClient.jetStream();


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
        latch.await();
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
        latch.await();
        if (error.get() != null) {
            fail();
        }
        return stream.get();
    }

    @Test
    public void testSubWithQueueName() throws InterruptedException {

        final NatsClient natsClient = getNatsClient();


        final CountDownLatch latch = new CountDownLatch(10);
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(20);
        final String data = "data";

        natsClient.subscribe(SUBJECT_NAME, "FOO", event -> {
            queue.add(event);
            latch.countDown();
        });

        for (int i = 0; i < 10; i++) {
            nc.publish(SUBJECT_NAME, (data + i).getBytes());
        }

        latch.await();

        assertEquals(10, queue.size());
        closeClient(natsClient);
    }

    @Test
    public void testForceFail() throws InterruptedException {

        try {
            final NatsClient natsClient = getNatsClient(port + 1);
            fail();
        } catch (Exception ex) {

        }
    }

    private void closeClient(NatsClient natsClient) throws InterruptedException {
        final CountDownLatch endLatch = new CountDownLatch(1);
        natsClient.end().onSuccess(event -> endLatch.countDown());
        endLatch.await(3, TimeUnit.SECONDS);
    }

    private NatsClient getNatsClient() throws InterruptedException {
       return getNatsClient(port);
    }
    private NatsClient getNatsClient(int port) throws InterruptedException {
        final NatsOptions natsOptions = new NatsOptions();
        natsOptions.getNatsBuilder().servers(new String[]{"localhost:" + port});
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
        latch.await();
        if (error.get() != null) {
            throw new IllegalStateException(error.get());
        }
        return natsClient;
    }
}
