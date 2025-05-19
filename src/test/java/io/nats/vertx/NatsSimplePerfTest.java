package io.nats.vertx;

import io.nats.client.Connection;
import io.nats.client.NUID;
import io.nats.client.Options;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class NatsSimplePerfTest {

    static class StreamHelper implements AutoCloseable {
        final String stream;
        final String subject;
        final TestsRunner runner;
        final Connection nc;
        final NatsClient natsClient;
        final NatsStream natsStream;

        public StreamHelper() throws Exception {
            stream = "PERF" + NUID.nextGlobal();
            subject = "S" + NUID.nextGlobalSequence();
            runner = TestsRunner.instance(1);
            nc = runner.nc;
            nc.jetStreamManagement()
                .addStream(StreamConfiguration.builder()
                    .name(stream)
                    .subjects(subject)
                    .storageType(StorageType.Memory)
                    .build());
            natsClient = getNatsClient();
            natsStream = getJetStream();
        }

        @Override
        public void close() throws Exception {
            try {
                closeClient();
            }
            catch (Exception ignore) {
            }

            try {
                runner.close();
            }
            catch (Exception ignore) {
            }
        }

        private NatsClient getNatsClient() throws InterruptedException {
            final NatsOptions natsOptions = new NatsOptions();
            natsOptions.setVertx(Vertx.vertx());
            natsOptions.setNatsBuilder(new Options.Builder());
            natsOptions.getNatsBuilder()
                .server("localhost:" + runner.port);
//                .connectionListener((conn, type) -> System.out.println("Connection EVENT " + type));
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

        private NatsStream getJetStream() throws InterruptedException {
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

        private void closeClient() throws InterruptedException {
            final CountDownLatch endLatch = new CountDownLatch(1);
            natsClient.end().onSuccess(event -> endLatch.countDown());
            endLatch.await(3, TimeUnit.SECONDS);
        }
    }

    @Test
    @Disabled // server 2.11 and later, this might fail on slow machines (i.e., CI machines) because publishing too fast
    public void testSubJetStream() throws Exception {
        testJetStreamPub(100_000, 10000, 10);
    }

    @Test
    public void testSubJetStream1000() throws Exception {
        testJetStreamPub(1000, 200, 3);

    }

    @Test
    public void testSubJetStream2000() throws Exception {
        testJetStreamPub(2_000, 1000, 3);

    }

    @Test
    public void testSubJetStream4000() throws Exception {
        testJetStreamPub(2_000, 1000, 3);
    }

    @Test
    public void testSubJetStream8000() throws Exception {
        testJetStreamPub(8_000, 1000, 3);
    }

    @Test
    public void testSubJetStream16000() throws Exception {
        testJetStreamPub(16_000, 1000, 3);
    }

    private void testJetStreamPub(int recordCount,
                                  int timeoutMS,
                                  long timeout) throws Exception {
        try (StreamHelper sh = new StreamHelper()) {

            final CountDownLatch latch = new CountDownLatch(recordCount);
            final String data = "data";

            sh.natsStream.subscribe(sh.subject,
                event -> latch.countDown(),
                true,
                new PushSubscribeOptions.Builder().build());

            final long startTime = System.currentTimeMillis();

            for (int i = 0; i < recordCount; i++) {
                sh.nc.publish(sh.subject, (data + i).getBytes());
            }

            latch.await(timeout, TimeUnit.SECONDS);

            final long endTime = System.currentTimeMillis();

            assertEquals(0, latch.getCount());

            System.out.printf("wrote record count %d in time ms %d \n", recordCount, (endTime - startTime));

            assertTrue((endTime - startTime) < timeoutMS);
        }
    }

}
