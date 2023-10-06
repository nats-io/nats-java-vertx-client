package io.nats.vertx;

import io.nats.client.*;
import io.nats.client.support.Status;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import nats.io.NatsServerRunner;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.fail;

public class TestUtils {

    public static NatsStream jetStream(NatsClient natsClient) {
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


     public static void closeClient(NatsClient natsClient) {
        final CountDownLatch endLatch = new CountDownLatch(1);
        natsClient.end().onSuccess(event -> endLatch.countDown());
        try {
            endLatch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public static NatsClient natsClient(int port) {
        return natsClient(port, Vertx.vertx(), System.err::println);
    }

    public static NatsClient natsClient(int port, Vertx vertx, Handler<Throwable> exceptionHandler) {
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



    public static NatsServerRunner startServer() throws Exception {
        NatsServerRunner.setDefaultOutputLevel(Level.WARNING);
        NatsServerRunner natsServerRunner = new NatsServerRunner(0, false, true);

        int port = natsServerRunner.getPort();
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
            latch.await(200, TimeUnit.MILLISECONDS);
            if (lastEvent.get() == ConnectionListener.Events.CONNECTED) {
                connect.close();
                break;
            }

        }

        return natsServerRunner;
    }

}
