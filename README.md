# Vert.x NATS client

This component provides a NATS client for reading and sending messages from/to a 
[https://nats.io](NATS) server. The client supports both Core NATS as well as JetStream.

## Using the Vert.x NATS client

To use this component, add the following dependency to the dependencies section of your build descriptor:

#### Maven (in your `pom.xml`):


```xml
<dependency>
  <groupId>io.nats</groupId>
  <artifactId>nats-vertx-nats-interface</artifactId>
  <version>${maven.version}</version>
</dependency>
```

#### Gradle (in your `build.gradle` file):

```
compile io.nats:nats-vertx-nats-interface:${maven.version}
```

## Core NATS

To send or receive messages to/from the NATS server you first need to create a client and connect.


```java
// Set options
NatsOptions config = new NatsOptions();
String[] servers = {"nats://myhost:4222"};
config.setServers(servers);
config.setMaxReconnects(3);


// Create client
NatsClient client = NatsClient.create(vertx, config);

// Connect
client
.connect()
.onSuccess(v ->
System.out.println("NATS successfully connected!"))
.onFailure(err ->
System.out.println("Fail to connect to NATS " + err.getMessage()))
```

### Publishing

Once connected, publishing is accomplished via one of three methods:

1) With a subject and message body:
```java
client
.publish("subject", "hello world".getBytes(StandardCharsets.UTF_8))
.onSuccess(v ->
System.out.println("Message published!"))
.onFailure(err ->
System.out.println("Something went wrong " + err.getMessage()));
```

2) With a subject and message body, as well as a subject for the receiver to reply to:

```java
client
.publish("subject", "replyto", "hello world".getBytes(StandardCharsets.UTF_8))
.onSuccess(v ->
System.out.println("Message published!"))
.onFailure(err ->
System.out.println("Something went wrong " + err.getMessage()));
```

3) When a requests expects a reply, a response is provided. Under the covers a request/reply pair is the same as a publish/subscribe only the library manages the subscription for you.

```java

client
.request("subject", "hello world".getBytes(StandardCharsets.UTF_8))
.onSuccess(response ->
System.out.println("Received response " + response.getData()))
.onFailure(err ->
System.out.println("Something went wrong " + err.getMessage()));
```

All of these methods, as well as the incoming message code use byte arrays for maximum flexibility. Applications can send JSON, Strings, YAML, Protocol Buffers, or any other format through NATS to applications written in a wide range of languages.

### Subscribing

The Java NATS library also provides a mechanisms to listen for messages.

```java

MessageHandler handler = (msg) -> {
// Process the message.
// Ack the message depending on the ack model
};

client
.subscribe("subject",handler)
.onSuccess(done ->
System.out.println("Subscribed successfully."))
NatsReceiver receiver = done.result();
receiver.handler(message -> {
System.out.println("Message received " + message.getData());
});
.onFailure(err ->
System.out.println("Something went wrong " + err.getMessage()));
```

Unsubscribing from messages.

```java

client
.unsubscribe("subject",
.onSuccess(done ->
System.out.println("Unsubscribed successfully."))
.onFailure(err ->
System.out.println("Something went wrong " + err.getMessage()));
```

## JetStream

Publishing and subscribing to JetStream enabled servers is straightforward. A JetStream enabled application will connect to a server, establish a JetStream context, and then publish or subscribe. This can be mixed and matched with standard NATS subject, and JetStream subscribers, depending on configuration, receive messages from both streams and directly from other NATS producers.

### The JetStream Context

After establishing a connection as described above, create a JetStream Context.

```java

JetStreamClient js = client.JetStream();
```

You can pass options to configure the JetStream client, although the defaults should suffice for most users. See the JetStreamOptions class.

There is no limit to the number of contexts used, although normally one would only require a single context.

### Publishing

To publish messages, use the `publish` method.


```java

// create a typical NATS message
Message msg = NatsMessage.builder()
.subject("foo")
.data("hello", StandardCharsets.UTF_8)
.build();

js
.publish(msg)
.onSuccess( pAck ->
System.out.println("Message published!"))
.onFailure(err ->
System.out.println("Something went wrong " + err.getMessage()));
```

There are a variety of publish options that can be set when publishing. When duplicate checking has been enabled on the stream, a message ID should be set. One set of options are expectations. You can set a publish expectation such as a particular stream name, previous message ID, or previous sequence number. These are hints to the server that it should reject messages where these are not met, primarily for enforcing your ordering or ensuring messages are not stored on the wrong stream.

void publish(Message data, Handler<AsyncResult<Void>> handler);
Future<Void> publish(Message data);
Future<Void> publish(String subject, String replyTo, String message);
Future<Void> publish(String subject, String message);

### Subscribing

There are two methods of subscribing, Push and Pull with each variety having its own set of options and abilities.

#### Push Subscribing

Push subscriptions are asynchronous. The server pushes messages to the client.

```java

MessageHandler handler = (msg) -> {
// Process the message.
// Ack the message depending on the ack model
};

PushSubscribeOptions so = PushSubscribeOptions.builder()
.durable("optional-durable-name")
.build();

boolean autoAck = ...

js.subscribe("my-subject", handler, autoAck, so)
.onSuccess(done ->
System.out.println("Subscribe success."))
.onFailure(err ->
System.out.println("Something went wrong " + err.getMessage()));

#### Pull Subscribing

Pull subscriptions are always synchronous. The server organizes messages into a batch which it sends when requested.
[source,java]
----
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
            m.ack(); //TODO: make async
          }        
        )
        .onFailure(err ->
          System.out.println("Something went wrong " + err.getMessage()))
      
    .onFailure(err ->
      System.out.println("Something went wrong " + err.getMessage()));
```

The fetch pull is a macro pull that uses advanced pulls under the covers to return a list of messages. The list may be empty or contain at most the batch size. All status messages are handled for you. The client can provide a timeout to wait for the first message in a batch. The timeout may be exceeded if the server sent messages very near the end of the timeout period.

#### Ordered Push Subscription Option

See https://github.com/nats-io/nats.java#ordered-push-subscription-option

#### Subscription Creation Checks

See https://github.com/nats-io/nats.java#subscription-creation-checks

#### Message Acknowledgements

There are multiple types of acknowledgements in JetStream:

* `Message.ack()`: Acknowledges a message.
* `Message.ackSync(Duration)`: Acknowledges a message and waits for a confirmation. When used with deduplications this creates exactly once delivery guarantees (within the deduplication window). This may significantly impact performance of the system.
* `Message.nak()`: A negative acknowledgment indicating processing failed and the message should be resent later.
* `Message.term()`: Never send this message again, regardless of configuration.
* `Message.inProgress()`: The message is being processed and reset the redelivery timer in the server. The message must be acknowledged later when processing is complete.

Note that exactly once delivery guarantee can be achieved by using a consumer with explicit ack mode attached to stream setup with a deduplication window and using the ackSync to acknowledge messages. The guarantee is only valid for the duration of the deduplication window.

## NATS Utils

To create a stream or update stream subject if the stream exists.


```java

// Set options
NatsOptions config = new NatsOptions();
config.setServer("nats://myhost:4222");
config.setMaxReconnects(3);
..
NatsUtils.createStreamOrUpdateSubjects(Connection connection, String streamName, NatsOptions config, String... subject);

```

