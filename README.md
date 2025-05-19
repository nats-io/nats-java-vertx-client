# Vert.x NATS client

This component provides a NATS client for reading and sending messages from/to a
[https://nats.io](NATS) server. The client supports both Core NATS and JetStream.

The nats-java-vertx-client is a Java client library for connecting to the NATS messaging system. It is built on top of
the Vert.x event-driven framework and provides an asynchronous, non-blocking API for sending and receiving messages over NATS.

**Current Release**: 2.0.3 &nbsp; **Current Snapshot**: 2.2.0-SNAPSHOT

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.nats/nats-vertx-interface/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.nats/nats-vertx-interface)
[![javadoc](https://javadoc.io/badge2/io.nats/nats-vertx-interface/javadoc.svg)](https://javadoc.io/doc/io.nats/nats-vertx-interface)
[![Coverage Status](https://coveralls.io/repos/github/nats-io/nats-java-vertx-client/badge.svg?branch=main)](https://coveralls.io/github/nats-io/nats-java-vertx-client?branch=main)
[![Build Main Badge](https://github.com/nats-io/nats-java-vertx-client/actions/workflows/build-main.yml/badge.svg?event=push)](https://github.com/nats-io/nats-java-vertx-client/actions/workflows/build-main.yml)
[![Release Badge](https://github.com/nats-io/nats-java-vertx-client/actions/workflows/build-release.yml/badge.svg?event=release)](https://github.com/nats-io/nats.java/actions/workflows/build-release.yml)

## Using the Vert.x NATS client

To use this component, add the following dependency to the dependencies section of your build descriptor:

#### Maven (in your `pom.xml`):


```xml
<dependency>
  <groupId>io.nats</groupId>
  <artifactId>nats-vertx-interface</artifactId>
  <version>${maven.version}</version>
</dependency>
```

#### Gradle (in your `build.gradle` file):

```
compile io.nats:nats-vertx-interface:${maven.version}
```

## Connecting to NATS
To connect to NATS, you must first create a NatsClient object by specifying the NATS server URL
and any optional configuration options. Here is an example:

```java
        // Set options
        final NatsOptions natsOptions = new NatsOptions();
        natsOptions.getNatsBuilder().server("localhost:" + port);

        // Create client
        final NatsClient natsClient = NatsClient.create(natsOptions);
        final Future<Void> connect = natsClient.connect();

```
This code creates a NatsClient object, sets the configuration options, and connects to a NATS server.

The first two lines create a new NatsOptions object and set the NATS server to connect to using the servers method of the NatsBuilder object.
In this case, the server is set to "localhost" and the port variable is used to specify the port number.

The following two lines create a new NatsClient object using the `create` method and passing in the NatsOptions object as a parameter.
The connect method is then called on the NatsClient object to establish a connection to the NATS server.
This method returns a Future<Void> object, representing the connection attempt's asynchronous result.

```java 

NatsClient client = NatsClient.create(config.setVertx(vertx));

// Connect
client.connect()
    .onSuccess(v -> System.out.println("NATS successfully connected!"))
    .onFailure(err -> System.out.println("Fail to connect to NATS " + err.getMessage()));
```

This code creates a NatsClient object, sets the Vert.x instance, and connects to a NATS server using the Vert.x Future
interface to handle the asynchronous result.

Using the `create` method, the first line creates a new NatsClient object and passes in a NatsOptions configuration
object as a parameter. In this case, the configuration object is created by setting the Vert.x instance to use with
the NATS client. The vertx is an instance of the Vert.x class.
The following few lines establish a connection to the NATS server using the connect method of the NatsClient object.
This method returns a `Future<Void>` object, representing the connection attempt's asynchronous result.
The `onSuccess` and `onFailure` methods of the Future object are then called (one or the other depending on the outcome)
to handle the result of the connection attempt.
In this example, we just print a message to the console to indicate whether the connection succeeded or failed.

### Publishing

Once connected, publishing is achieved via one of three methods:

1) With a subject and message body:

```java
    client
    .publish("subject", "hello world".getBytes(StandardCharsets.UTF_8))
    .onSuccess(v ->
    System.out.println("Message published!"))
    .onFailure(err ->
    System.out.println("Something went wrong " + err.getMessage()));
```

This code publishes a message to a NATS subject using the `publish` method of a `NatsClient` object, and handles the 
asynchronous result of the operation using the Vert.x Future interface.

The `publish` method takes two parameters: the subject to publish the message to, and the message data as a byte array.

The onSuccess and onFailure methods are called on the Future object returned by the `publish` method, and are used to
handle the asynchronous result of the operation. In this example, we're just printing a message to the console to
indicate whether the operation succeeded or failed.

If the operation succeeds, the `onSuccess` method is called with a Void parameter. In this case, we're just printing the
message "Message published!" to the console.

If the operation fails, the onFailure method is called with a Throwable parameter. In this case, we're just printing a
message to the console that includes the error message returned by the Throwable object.


2) With a subject and message body, as well as a subject for the receiver to reply to:

```java
    client
    .publish("subject", "replyto", "hello world".getBytes(StandardCharsets.UTF_8))
    .onSuccess(v -> System.out.println("Message published!"))
    .onFailure(err ->   System.out.println("Something went wrong " + err.getMessage()));
```

This code publishes a message to a NATS subject and specifies a reply-to subject to use for any responses received in
reply to the message. It also handles the asynchronous result of the operation using the Vert.x Future interface.

The publish method takes three parameters: the subject to publish the message to, the reply-to subject to use for any
responses, and the message data as a byte array.

The `onSuccess` and `onFailure` methods are called on the `Future` object returned by the `publish` method, and are used to
handle the asynchronous result of the operation. In this example, we're just printing a message to the console to
indicate whether the operation succeeded or failed.

If the operation succeeds, the `onSuccess` method is called with a Void parameter. In this case, we're just printing the
message "Message published!" to the console.

If the operation fails, the `onFailure` method is called with a Throwable parameter. In this case, we're just printing
a message to the console that includes the error message returned by the Throwable object.

3) When a request expects a reply, a response is provided. 
Under the covers as a request/reply pair is the same as a publish/subscribe, only the library manages the subscription for you.

```java

client
    .request("subject", "hello world".getBytes(StandardCharsets.UTF_8))
    .onSuccess(response ->
    System.out.println("Received response " + response.getData()))
    .onFailure(err ->
    System.out.println("Something went wrong " + err.getMessage()));
```

When a request is made and a reply is expected, the NATS messaging system provides a response. This is achieved through
a request/reply pair. This is the same as a publish/subscribe pair, except the library handles the subscription for you.

This code makes a request to a NATS subject and handles a response using the request method of a NatsClient object,
and handles the asynchronous result of the operation using the Vert.x `Future` interface.

The request method takes two parameters: the subject to send the request to, and the message data as a byte array.
When a response is received, the onSuccess method is called with a Message parameter, which contains the response data as a byte array.


If the operation fails, the `onFailure` method is called with a `Throwable` parameter. In this case, we're just printing a
message to the console that includes the error message returned by the Throwable object.

All of these methods, as well as the incoming message code, use byte arrays for maximum flexibility.
Applications can send JSON, Strings, YAML, Protocol Buffers, or any other format through NATS to
applications written in a wide range of languages.

### Subscribing

The Java NATS library also provides a mechanism to listen to messages.

```java

natsClient.subscribe(SUBJECT_NAME, "FOO", event -> {
doSomethingWithTheEvent(event);
        
        });
```

Unsubscribing from messages.

```java

client.unsubscribe(SUBJECT_NAME)
        .onFailure(Throwable::printStackTrace)
        .onSuccess(event -> System.out.println("Success"));
```

## JetStream

Publishing and subscribing to a JetStream-enabled server is straightforward. A JetStream-enabled application
will connect to a server, establish a JetStream context, and then publish or subscribe. This can be mixed and matched
with a standard NATS subject, and JetStream subscribers, depending on configuration, receive messages from
both streams and directly from other NATS producers.

### The JetStream Context

After establishing a connection as described above, create a JetStream Context.

```java

final Future<NatsStream> streamFuture = natsClient.jetStream();

streamFuture.onSuccess(natsStream -> {
doSomethingWithStream(natsStream)
        }).onFailure(error -> {
handleTheStreamFailed(error);
        });
```

You can pass options to configure the JetStream client, although the defaults should suffice for most users. See the JetStreamOptions class.

There is no limit to the number of contexts used, although normally, one would only require a single context.

### Publishing

To publish messages, use the `publish` method.


```java



final NatsStream jetStreamPub = ...
final NatsStream jetStreamSub = ...

final String data = "data";

jetStreamSub.subscribe(SUBJECT_NAME, event -> {
doSomethingWithMessage(event.message());

    }, true, PushSubscribeOptions.builder().build());


// Send a message
final NatsMessage message = NatsMessage.builder()
    .subject(SUBJECT_NAME)
    .data(data + i, StandardCharsets.UTF_8).build();
            jetStreamPub.publish(message).onSuccess(event -> ...).onError(error -> ...);

```

To unsubscribe from JetStream the interface is similar to unsubscribing to a NATS subscription.


```java
        jetStreamSub.unsubscribe(SUBJECT_NAME).onSuccess(event -> System.out.println("Unsubscribed"))
    .onFailure(Throwable::printStackTrace);
```

There are a variety of publish options that can be set when publishing.
When duplicate checking has been enabled on the stream, a message ID should be set.
One set of options is expectations. You can set a publish expectation such as a particular stream name,
previous message ID, or previous sequence number. These are hints to the server that it should reject messages
where these are not met, primarily for enforcing your ordering or ensuring messages are not stored on the wrong stream.

```java

void publish(Message data, Handler<AsyncResult<Void>> handler);
Future<Void> publish(Message data);
Future<Void> publish(String subject, String replyTo, String message);
Future<Void> publish(String subject, String message);

```


### Subscribing

There are two methods of subscribing, Push and Pull, with each variety having its own set of options and abilities.

#### Push Subscribing

Push subscriptions are asynchronous. The server pushes messages to the client.

```java


PushSubscribeOptions so = PushSubscribeOptions.builder()
    .durable("optional-durable-name")
    .build();

boolean autoAck = ...

    js.subscribe("my-subject", (msg) -> {
// Process the message.
// Ack the message depending on the ack model
    }, autoAck, so)
    .onSuccess(done ->
    System.out.println("Subscribe success."))
    .onFailure(err ->
    System.out.println("Something went wrong " + err.getMessage()));

```

#### Pull Subscribing

Pull subscriptions are always synchronous. The server organizes messages into a batch that it sends when requested.

```java 
PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
    .durable("durable-name-is-required")
    .build();

js.subscribe("subject", pullOptions)
.onSuccess(done ->
    System.out.println("Subscribe success.")
JetStreamSubscription sub = done.result()

      sub
          .fetch(100, Duration.ofSeconds(1))
    .onSuccess(messages ->
    for (Message m : messages) {
    // process message
    m.ackAsync().onSuccess(e -> ...).onError(err -> ...);
    }
    )
    .onFailure(err ->
    System.out.println("Something went wrong " + err.getMessage()))

    .onFailure(err ->
    System.out.println("Something went wrong " + err.getMessage()));
```

The fetch pull is a macro pull that uses advanced pulls under the covers to return a list of messages.
The list may be empty or contain at most the batch size. All status messages are handled for you.
The client can provide a timeout to wait for the first message in a batch.
The timeout may be exceeded if the server sends messages very near the end of the timeout period.

#### Ordered Push Subscription Option

See https://github.com/nats-io/nats.java#ordered-push-subscription-option

#### Subscription Creation Checks

See https://github.com/nats-io/nats.java#subscription-creation-checks

#### Message Acknowledgements

There are multiple types of acknowledgments in JetStream:

* `Message.ackAsync()`: Acknowledges a message.
* `Message.ackSync(Duration)`: Acknowledges a message and waits for a confirmation. When used with deduplication, this creates exactly once delivery guarantees (within the deduplication window). This deduplication may significantly impact the performance of the system.
* `Message.nakAsync()`: A negative acknowledgment indicating processing failed, and the message should be resent later.
* `Message.termAsync()`: Never send this message again, regardless of configuration.
* `Message.inProgressAsync()`: The message is being processed, and reset the redelivery timer in the server. The message must be acknowledged later when processing is complete.

Note that the exactly once delivery guarantee can be achieved by using a consumer with explicit ack mode attached to stream setup with a deduplication window and using the ackSync to acknowledge messages. The guarantee is only valid for the duration of the deduplication window.

You should always use the async versions of the methods when running in the vert.x event loop.

## Conclusion
The nats-java-vertx-client library provides a simple and easy-to-use API for connecting to NATS messaging system from
Java applications, using the Vert.x interface. With the asynchronous, non-blocking API and Vert.x event-driven framework,
it is well-suited for building high-performance, scalable messaging applications.
