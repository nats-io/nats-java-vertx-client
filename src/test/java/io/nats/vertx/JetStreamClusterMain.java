package io.nats.vertx;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.support.Status;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public class JetStreamClusterMain {

    private static final String SUBJECT_NAME = "mytest5";
    private static final AtomicBoolean stop = new AtomicBoolean();
    private static final LongAdder consumerMessageCount = new LongAdder();
    private static final LongAdder producerMessageCount = new LongAdder();
    private static final LongAdder errorCount = new LongAdder();

    public static void main (String... args) throws  Exception {
        final JetStream jetStreamConsumer = getJetStream();
        final JetStream jetStreamProducer = getJetStream();


        final Connection connection = getConnection();

        final JetStreamManagement jsm = connection.jetStreamManagement();
        StreamInfo streamInfo = null;
        try {
             streamInfo = jsm.getStreamInfo(SUBJECT_NAME);
            System.out.printf("%s %s %s\n", streamInfo, streamInfo.getClusterInfo(), streamInfo.getStreamState());
            jsm.purgeStream(SUBJECT_NAME);
        } catch (Exception ex) {
            System.out.printf("Unable to lookup stream %s \n", ex.getMessage());
        }
        if (streamInfo == null) {
            StreamConfiguration sc = StreamConfiguration.builder().name(SUBJECT_NAME)
                    .storageType(StorageType.File).replicas(3).build();
            streamInfo = jsm.addStream(sc);
            System.out.printf("%s\n%s\n%s\n", streamInfo, streamInfo.getClusterInfo(), streamInfo.getStreamState());
        }
        Thread.sleep(1000);

        Executors.newCachedThreadPool().submit(
                () -> {
                    try {
                        handleConsumer(jetStreamConsumer);
                    } catch (Exception ex) {
                        System.out.printf("Problem running consumer %s \n", ex.getMessage());
                    }
                }
        );

        Executors.newCachedThreadPool().submit(
                () -> {
                    try {
                        handleProducer(jetStreamProducer);
                    } catch (Exception ex) {
                        System.out.println("Problem running producer");
                        ex.printStackTrace();
                    }
                }
        );

        Executors.newCachedThreadPool().submit(
                () -> {
                    try {
                        while(true) {
                            System.out.printf("Status       sent %d         received %d         errors %d \n",
                                    producerMessageCount.sum(), consumerMessageCount.sum(), errorCount.sum());
                            if (stop.get()) {
                                break;
                            }
                            Thread.sleep(10_000);
                        }

                    } catch (Exception ex) {
                        System.out.println("Problem running status");
                        ex.printStackTrace();
                    }
                }
        );
        System.out.println("DONE");

    }

    private static void handleProducer(final JetStream jetStreamProducer) throws Exception {


        while (true) {
            try {
                jetStreamProducer.publish(SUBJECT_NAME, "data".getBytes(StandardCharsets.UTF_8));
                producerMessageCount.increment();
                //Thread.sleep(3000);
                if (stop.get()) {
                    break;
                }
            } catch (Exception ex) {
                System.out.printf("handleProducer: %s %s \n", ex.getMessage(), ex.getClass().getName());
                Thread.sleep(7000);
            }
        }
    }

    private static void handleConsumer(final JetStream jetStreamConsumer) throws Exception {
        final JetStreamSubscription subscription = jetStreamConsumer.subscribe(SUBJECT_NAME);
        while (true) {
            try {

                Message message = subscription.nextMessage(1000);

                while (message != null) {
                    message.ack();
                    consumerMessageCount.increment();
                    message = subscription.nextMessage(1000);
                }
                if (stop.get()) {
                    break;
                }
                Thread.sleep(1000);
            }catch (Exception ex) {
                System.out.printf("handleConsumer: %s %s \n", ex.getMessage(), ex.getClass().getName());
                Thread.sleep(7000);
            }
        }
    }

    private static JetStream getJetStream() throws IOException, InterruptedException {
        final Connection connection = getConnection();
        final JetStream jetStream = connection.jetStream();
        return jetStream;
    }

    private static Connection getConnection() throws IOException, InterruptedException {
        final Options options = new Options.Builder().connectionTimeout(Duration.ofSeconds(5))
                .servers(new String[]{"nats://localhost:4221", "nats://localhost:4222",
                "nats://localhost:4223"}).connectionListener(new ConnectionListener() {
            @Override
            public void connectionEvent(Connection conn, Events type) {
                System.out.printf("CE %s %s \n", conn, type);
            }
        }).errorListener(new ErrorListener() {
            @Override
            public void errorOccurred(Connection conn, String error) {
                System.out.printf("ERR %s %s \n", conn, error);
                errorCount.increment();
            }

            @Override
            public void exceptionOccurred(Connection conn, Exception exp) {
                System.out.printf("EXP %s %s \n", conn, exp);
                errorCount.increment();
            }

            @Override
            public void slowConsumerDetected(Connection conn, Consumer consumer) {
                System.out.printf("EO-SLOW %s %s \n", conn, consumer);
            }

            @Override
            public void messageDiscarded(Connection conn, Message msg) {
                System.out.printf("EO-DISCARD %s %s \n", conn, msg);
            }

            @Override
            public void heartbeatAlarm(Connection conn, JetStreamSubscription sub, long lastStreamSequence, long lastConsumerSequence) {
                System.out.printf("EO-ALARM %s %s %s %s\n", conn, sub, lastConsumerSequence, lastConsumerSequence);
            }

            @Override
            public void unhandledStatus(Connection conn, JetStreamSubscription sub, Status status) {
                System.out.printf("EO-U_STAT %s %s %s\n", conn, sub, status);
            }

            @Override
            public void flowControlProcessed(Connection conn, JetStreamSubscription sub, String subject, FlowControlSource source) {
                System.out.printf("EO-FLOW %s %s %s %s\n", conn, sub, subject, source);
            }
        }).build();

        final Connection connection = Nats.connect(options);
        return connection;
    }
}
