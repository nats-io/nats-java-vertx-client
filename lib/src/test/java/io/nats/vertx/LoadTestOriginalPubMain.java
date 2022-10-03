package io.nats.vertx;

import io.nats.client.Connection;
import io.nats.client.JetStream;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class LoadTestOriginalPubMain extends LoadTestOriginalMain{


    public final static byte[] DATA = "0".getBytes(StandardCharsets.UTF_8);

    public static void main(String[] args) throws Exception {


        setup(PORT);
        final Connection natsClient = getNatsClient(PORT);
        final JetStream jetStream = getJetStream(natsClient);


        for (int i = 0; i < MESSAGE_COUNT; i++) {
            jetStream.publish(SUBJECT_NAME, DATA);
        }

        closeClient(natsClient);
    }
}
