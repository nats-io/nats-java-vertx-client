package io.nats.vertx.impl;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsKeyValueWatchSubscription;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.Validator;
import io.nats.vertx.NatsVertxKeyValue;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.nats.client.support.NatsConstants.GREATER_THAN;
import static io.nats.client.support.NatsJetStreamConstants.*;
import static io.nats.client.support.NatsKeyValueUtil.*;
import static io.nats.client.support.Validator.*;


/**
 * NATS Client implementation.
 */
public class NatsVertxKeyValueImpl extends NatsImpl implements NatsVertxKeyValue {

    // JNats KeyValue parallel variables
    private final String bucketName;
    private final String streamName;
    private final String streamSubject;
    private final String readPrefix;
    private final String writePrefix;
    private final KeyValue kv;

    /**
     * Create instance
     *
     * @param conn             Nats connection
     * @param vertx            vertx
     * @param exceptionHandler
     * @param bucketName
     * @param kvo              keyValueOptions also contains jetStreamOptions use to make jetstream/management implementations
     */
    public NatsVertxKeyValueImpl(final Connection conn,
                                 final Vertx vertx,
                                 final Handler<Throwable> exceptionHandler,
                                 final String bucketName,
                                 final KeyValueOptions kvo)
    {
        super(conn, vertx, exceptionHandler, kvo == null ? null : kvo.getJetStreamOptions());

        this.bucketName = Validator.validateBucketName(bucketName, true);
        streamName = toStreamName(bucketName);
        streamSubject = toStreamSubject(bucketName);
        readPrefix = toKeyPrefix(bucketName);

        if (kvo == null || kvo.getJetStreamOptions().isDefaultPrefix()) {
            writePrefix = readPrefix;
        }
        else {
            writePrefix = kvo.getJetStreamOptions().getPrefix() + readPrefix;

        }
        try {
            this.kv = conn.keyValue(bucketName, kvo);
        }
        catch (IOException e) {
            if (exceptionHandler != null) {
                exceptionHandler.handle(e);
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public NatsImpl getImpl() {
        return this;
    }

    @Override
    public NatsVertxKeyValueImpl exceptionHandler(Handler<Throwable> handler) {
        exceptionHandler.set(handler);
        return this;
    }

    @Override
    public String readSubject(String key) {
        return readPrefix + key;
    }

    @Override
    public String writeSubject(String key) {
        return writePrefix + key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getBucketName() {
        return bucketName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<KeyValueEntry> get(String key) {
        return _getFuture(key, null, true); // null indicates get last, not get revision
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<KeyValueEntry> get(String key, long revision) {
        return _getFuture(key, revision, true);
    }

    @Override
    public Future<Long> put(String key, byte[] value) {
        return publishReturnSequence(key, value, null);
    }

    @Override
    public Future<Long> put(String key, String value) {
        return publishReturnSequence(key, value.getBytes(StandardCharsets.UTF_8), null);
    }

    @Override
    public Future<Long> put(String key, Number value) {
        return publishReturnSequence(key, value.toString().getBytes(StandardCharsets.US_ASCII), null);
    }

    @Override
    public Future<Long> create(String key, byte[] value) {
        final Promise<Long> promise = context().promise();
        context().executeBlocking(event -> {
            try {
                try {
                    Headers h = new Headers().add(EXPECTED_LAST_SUB_SEQ_HDR, Long.toString(0));
                    PublishAck pa = _publish(key, value, h);
                    promise.complete(pa.getSeqno());
                }
                catch (JetStreamApiException e) {
                    if (e.getApiErrorCode() == JS_WRONG_LAST_SEQUENCE) {
                        // must check if the last message for this subject is a delete or purge
                        KeyValueEntry kve = _getLastEntry(key, false);
                        if (kve != null && kve.getOperation() != KeyValueOperation.PUT) {
                            Headers h = new Headers().add(EXPECTED_LAST_SUB_SEQ_HDR, Long.toString(kve.getRevision()));
                            PublishAck pa = _publish(key, value, h);
                            promise.complete(pa.getSeqno());
                            return;
                        }
                    }
                    promise.fail(e);
                }
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    @Override
    public Future<Long> update(String key, byte[] value, long expectedRevision) {
        Headers h = new Headers().add(EXPECTED_LAST_SUB_SEQ_HDR, Long.toString(expectedRevision));
        return publishReturnSequence(key, value, h);
    }

    @Override
    public Future<Long> update(String key, String value, long expectedRevision) {
        return update(key, value.getBytes(StandardCharsets.UTF_8), expectedRevision);
    }

    @Override
    public Future<Void> delete(String key) {
        return publishReturnVoid(key, null, getDeleteHeaders());
    }

    @Override
    public Future<Void> delete(String key, long expectedRevision) {
        Headers h = getDeleteHeaders().put(EXPECTED_LAST_SUB_SEQ_HDR, Long.toString(expectedRevision));
        return publishReturnVoid(key, null, h);
    }

    @Override
    public Future<Void> purge(String key) {
        return publishReturnVoid(key, null, getPurgeHeaders());
    }

    @Override
    public Future<Void> purge(String key, long expectedRevision) {
        Headers h = getPurgeHeaders().put(EXPECTED_LAST_SUB_SEQ_HDR, Long.toString(expectedRevision));
        return publishReturnVoid(key, null, h);
    }

    @Override
    public Future<NatsKeyValueWatchSubscription> watch(String key, KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) {
        return watch(Collections.singletonList(key), watcher, -1, watchOptions);
    }

    @Override
    public Future<NatsKeyValueWatchSubscription> watch(String key, KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) {
        return watch(Collections.singletonList(key), watcher, fromRevision, watchOptions);
    }

    @Override
    public Future<NatsKeyValueWatchSubscription> watch(List<String> keys, KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) {
        return watch(keys, watcher, -1, watchOptions);
    }

    @Override
    public Future<NatsKeyValueWatchSubscription> watch(List<String> keys, KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) {
        final Promise<NatsKeyValueWatchSubscription> promise = context().promise();
        context().executeBlocking(event -> {
            try {
                validateKvKeysWildcardAllowedRequired(keys);
                validateNotNull(watcher, "Watcher is required");
                promise.complete(kv.watch(keys, watcher, fromRevision, watchOptions));
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    @Override
    public Future<NatsKeyValueWatchSubscription> watchAll(KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) {
        return watch(Collections.singletonList(GREATER_THAN), watcher, -1, watchOptions);
    }

    @Override
    public Future<NatsKeyValueWatchSubscription> watchAll(KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) {
        return watch(Collections.singletonList(GREATER_THAN), watcher, fromRevision, watchOptions);
    }

    public Future<List<String>> keys() {
        return _keys(Collections.singletonList(readSubject(GREATER_THAN)));
    }

    @Override
    public Future<List<String>> keys(String filter) {
        return _keys(Collections.singletonList(readSubject(filter)));
    }

    @Override
    public Future<List<String>> keys(List<String> filters) {
        List<String> readSubjectFilters = new ArrayList<>(filters.size());
        for (String f : filters) {
            readSubjectFilters.add(readSubject(f));
        }
        return _keys(readSubjectFilters);
    }

    private Future<List<String>> _keys(List<String> readSubjectFilters) {
        final Promise<List<String>> promise = context().promise();
        context().executeBlocking(event -> {
            try {
                List<String> list = new ArrayList<>();
                visitSubject(streamName, readSubjectFilters, DeliverPolicy.LastPerSubject, true, false, m -> {
                    KeyValueOperation op = getOperation(m.getHeaders());
                    if (op == KeyValueOperation.PUT) {
                        list.add(new BucketAndKey(m).key);
                    }
                });
                promise.complete(list);
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    @Override
    public Future<List<KeyValueEntry>> history(String key) {
        final Promise<List<KeyValueEntry>> promise = context().promise();
        context().executeBlocking(event -> {
            try {
                validateNonWildcardKvKeyRequired(key);
                List<KeyValueEntry> list = new ArrayList<>();
                visitSubject(streamName, readSubject(key), DeliverPolicy.All, false, true, m -> list.add(new KeyValueEntry(m)));
                promise.complete(list);
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    @Override
    public Future<Void> purgeDeletes() {
        return purgeDeletes(null);
    }

    @Override
    public Future<Void> purgeDeletes(KeyValuePurgeOptions options) {
        final Promise<Void> promise = context().promise();
        context().executeBlocking(event -> {
            try {
                long dmThresh = options == null
                    ? KeyValuePurgeOptions.DEFAULT_THRESHOLD_MILLIS
                    : options.getDeleteMarkersThresholdMillis();

                ZonedDateTime limit;
                if (dmThresh < 0) {
                    limit = DateTimeUtils.fromNow(600000); // long enough in the future to clear all
                }
                else if (dmThresh == 0) {
                    limit = DateTimeUtils.fromNow(KeyValuePurgeOptions.DEFAULT_THRESHOLD_MILLIS);
                }
                else {
                    limit = DateTimeUtils.fromNow(-dmThresh);
                }

                List<String> keep0List = new ArrayList<>();
                List<String> keep1List = new ArrayList<>();
                visitSubject(streamName, streamSubject, DeliverPolicy.LastPerSubject, true, false, m -> {
                    KeyValueEntry kve = new KeyValueEntry(m);
                    if (kve.getOperation() != KeyValueOperation.PUT) {
                        if (kve.getCreated().isAfter(limit)) {
                            keep1List.add(new BucketAndKey(m).key);
                        }
                        else {
                            keep0List.add(new BucketAndKey(m).key);
                        }
                    }
                });

                for (String key : keep0List) {
                    jsm.purgeStream(streamName, PurgeOptions.subject(readSubject(key)));
                }

                for (String key : keep1List) {
                    PurgeOptions po = PurgeOptions.builder()
                        .subject(readSubject(key))
                        .keep(1)
                        .build();
                    jsm.purgeStream(streamName, po);
                }
                promise.complete();
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    @Override
    public Future<KeyValueStatus> getStatus() {
        final Promise<KeyValueStatus> promise = context().promise();
        context().executeBlocking(event -> {
            try {
                promise.complete(new KeyValueStatus(jsm.getStreamInfo(streamName)));
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    private PublishAck _publish(String key, byte[] d, Headers h) throws IOException, JetStreamApiException {
        validateNonWildcardKvKeyRequired(key);
        Message m = NatsMessage.builder().subject(writeSubject(key)).data(d).headers(h).build();
        return js.publish(m);
    }

    private Future<Long> publishReturnSequence(String key, byte[] data, Headers h) {
        final Promise<Long> promise = context().promise();
        context().executeBlocking(event -> {
            try {
                PublishAck pa = _publish(key, data, h);
                promise.complete(pa.getSeqno());
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    private Future<Void> publishReturnVoid(String key, byte[] data, Headers h) {
        final Promise<Void> promise = context().promise();
        context().executeBlocking(event -> {
            try {
                _publish(key, data, h);
                promise.complete();
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    Future<KeyValueEntry> _getFuture(String key, Long revision, boolean existingOnly) {
        final Promise<KeyValueEntry> promise = context().promise();
        context().executeBlocking(event -> {
            try {
                validateNonWildcardKvKeyRequired(key);
                KeyValueEntry kve = revision == null
                    ? _getLastEntry(key, existingOnly)
                    : _getRevisionEntry(key, revision, existingOnly);
                promise.complete(kve);
            } catch (Exception e) {
                handleException(promise, e);
            }
        }, false);
        return promise.future();
    }

    private KeyValueEntry resolveExistingOnly(KeyValueEntry kve, boolean existingOnly) {
        return existingOnly && kve.getOperation() != KeyValueOperation.PUT ? null : kve;
    }

    private KeyValueEntry _getLastEntry(String key, boolean existingOnly) throws IOException, JetStreamApiException {
        MessageInfo mi = _getLastMi(readSubject(key));
        KeyValueEntry kve = mi == null ? null : new KeyValueEntry(mi);
        if (kve != null) {
            kve = resolveExistingOnly(kve, existingOnly);
        }
        return kve;
    }

    private KeyValueEntry _getRevisionEntry(String key, long revision, boolean existingOnly) throws IOException, JetStreamApiException {
        MessageInfo mi = _getRevisionMi(revision);
        KeyValueEntry kve = mi == null ? null : new KeyValueEntry(mi);
        if (kve != null) {
            if (key.equals(kve.getKey())) {
                kve = resolveExistingOnly(kve, existingOnly);
            }
            else {
                kve = null;
            }
        }
        return kve;
    }

    protected MessageInfo _getLastMi(String subject) throws IOException, JetStreamApiException {
        try {
            return jsm.getLastMessage(streamName, subject);
        }
        catch (JetStreamApiException jsae) {
            if (jsae.getApiErrorCode() == JS_NO_MESSAGE_FOUND_ERR) {
                return null;
            }
            throw jsae;
        }
    }

    protected MessageInfo _getRevisionMi(long seq) throws IOException, JetStreamApiException {
        try {
            return jsm.getMessage(streamName, seq);
        }
        catch (JetStreamApiException jsae) {
            if (jsae.getApiErrorCode() == JS_NO_MESSAGE_FOUND_ERR) {
                return null;
            }
            throw jsae;
        }
    }
}
