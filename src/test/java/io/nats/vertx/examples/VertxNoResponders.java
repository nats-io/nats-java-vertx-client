// Copyright 2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.vertx.examples;

import io.nats.client.Message;
import io.nats.client.Options;
import io.nats.vertx.NatsClient;
import io.nats.vertx.NatsOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * INSTRUCTIONS TO BUILD AND RUN
 * 1. gradlew clean compileJava copyToLib
 * 2. java -cp "build/libs/*;build/classes/java/main" io.nats.client.examples.VertxNoResponders
 */
public class VertxNoResponders {

    private static final String SEP = "-------------------------------------------------------";
    private static final String[] SERVERS = {"localhost:4222"};

    public static void main(String[] args) throws InterruptedException {
        NatsOptions minOptions = createNatsOptions("MIN", false);
        NatsClient minNatsClient = NatsClient.create(minOptions);
        connect(minNatsClient);

        NatsOptions noRespondersOptions = createNatsOptions("NR", true);
        NatsClient noRespondersNatsClient = NatsClient.create(noRespondersOptions);
        connect(noRespondersNatsClient);

        testWithFuture("Minimal", minNatsClient, false);
        testWithFuture("No Responders", noRespondersNatsClient, true);
        testWithoutFuture("Minimal", minNatsClient);
        testWithoutFuture("No Responders", noRespondersNatsClient);

        System.exit(0);
    }

    private static void testWithFuture(String label, NatsClient natsClient, boolean expectNoResponders) {
        System.out.println("\n" + SEP + "\nTest With Future, Connect with '" + label + "' Options\n" + SEP);
        requestWithFuture(natsClient, expectNoResponders);
    }

    private static void testWithoutFuture(String label, NatsClient natsClient) {
        System.out.println("\n" + SEP + "\nTest W/O Future, Connect with '" + label + "' Options\n" + SEP);
        requestWithoutFuture(natsClient);
    }

    private static void requestWithFuture(NatsClient natsClient, boolean expectNoResponders) {
        String subject = subject();
        byte[] data = data();
        System.out.println("Making request on subject '" + subject + "'");
        final CountDownLatch requestLatch = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        Future<Message> fRequest = natsClient.request(subject, data);
        fRequest
            .onSuccess(event -> {
                System.out.println("Should Not Succeed!");
                requestLatch.countDown();
            })
            .onFailure(event -> {
                error.set(event);
                requestLatch.countDown();
            });

        try {
            if (!requestLatch.await(1, TimeUnit.SECONDS)) {
                System.out.println("\nTook Too Long");
                System.exit(-1);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        if (expectNoResponders) {
            System.out.println("Should be No Responders: " + stringify(error.get()));
        }
        else {
            System.out.println("Should Be CancellationException: " + stringify(error.get()));
        }
    }

    private static void requestWithoutFuture(NatsClient natsClient) {
        String subject = subject();
        byte[] data = data();
        System.out.println("Making request on subject '" + subject + "'");
        final CountDownLatch requestLatch = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        Future<Message> fRequest = natsClient.request(subject, data, Duration.ofMillis(500));
        fRequest
            .onSuccess(event -> {
                System.out.println("Should Not Succeed!");
                requestLatch.countDown();
            })
            .onFailure(event -> {
                error.set(event);
                requestLatch.countDown();
            });

        try {
            if (!requestLatch.await(1, TimeUnit.SECONDS)) {
                System.out.println("\nTook Too Long");
                System.exit(-1);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        System.out.println("Should be No Responders: " + stringify(error.get()));
    }

    private static NatsOptions createNatsOptions(String label, boolean reportNoResponders) {
        NatsOptions natsOptions = new NatsOptions();
        Options.Builder builder = Options.builder()
            .connectionTimeout(Duration.ofSeconds(3))
            .servers(SERVERS);
        if (reportNoResponders) {
            builder.reportNoResponders();
        }
        natsOptions.setNatsBuilder(builder);
        natsOptions.setVertx(Vertx.vertx());
        natsOptions.setExceptionHandler(t -> System.out.println(label + " Error Handler: "  + stringify(t)));
        return natsOptions;
    }

    private static void connect(NatsClient natsClient) throws InterruptedException {
        final CountDownLatch connectLatch = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();

        Future<Void> fConnect = natsClient.connect();
        fConnect
            .onSuccess(event -> connectLatch.countDown())
            .onFailure(event -> {
                error.set(event);
                connectLatch.countDown();
            });
        if (!connectLatch.await(3, TimeUnit.SECONDS) || error.get() != null) {
            System.out.println("\nConnection Failed");
            System.exit(-1);
        }
    }

    private static String stringify(Throwable t) {
        if (t.getCause() == null) {
            return "\n  " + t;
        }
        return "\n  " + t.toString().replaceFirst(": ", ":\n    ");
    }

    static AtomicInteger S = new AtomicInteger(0xA0);
    private static String subject() {
        return "S-" + Integer.toHexString(S.incrementAndGet()).toUpperCase();
    }

    static AtomicInteger D = new AtomicInteger(0xD0);
    private static byte[] data() {
        return ("D-" + Integer.toHexString(D.incrementAndGet()).toUpperCase()).getBytes(StandardCharsets.UTF_8);
    }
}
