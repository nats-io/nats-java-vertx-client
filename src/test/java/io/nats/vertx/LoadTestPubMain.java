package io.nats.vertx;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class LoadTestPubMain extends LoadTestMain {


    public static void main(String[] args) throws Exception {


        setup(PORT);
        final NatsClient natsClient = getNatsClient(PORT);
        final NatsStream jetStream = getJetStream(natsClient);
        final CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);
        final LongAdder counter = new LongAdder();

        final long startTime = System.currentTimeMillis();


        for (int i = 0; i < MESSAGE_COUNT; i++) {
            jetStream.publish(SUBJECT_NAME, DATA).onSuccess(handler-> {
                latch.countDown();
                counter.increment();
            });
        }

        if (latch.await(30, TimeUnit.SECONDS)) {
            final long endTime = System.currentTimeMillis();


            final long duration = endTime - startTime;
            System.out.printf("PUB %s %d %,d %d \n", SUBJECT_NAME, MESSAGE_COUNT, counter.sum(), duration);
        } else {
            final long endTime = System.currentTimeMillis();
            final long duration = endTime - startTime;
            System.out.printf("FAILED PUB %s %d %,d %d \n", SUBJECT_NAME, MESSAGE_COUNT, counter.sum(), duration);
        }

        closeClient(natsClient);
        VERTX.close();
    }

}