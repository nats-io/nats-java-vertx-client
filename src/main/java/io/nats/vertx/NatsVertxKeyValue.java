package io.nats.vertx;

import io.nats.client.api.*;
import io.nats.client.impl.NatsKeyValueWatchSubscription;
import io.nats.vertx.impl.NatsImpl;
import io.vertx.core.Future;
import io.vertx.core.streams.StreamBase;

import java.util.List;

/**
 * Provides a Vert.x WriteStream interface with Futures and Promises.
 */
public interface NatsVertxKeyValue extends StreamBase {

    /**
     * Get the name of the bucket.
     * @return the name
     */
    String getBucketName();

    /**
     * Get the entry for a key
     * when the key exists and is live (not deleted and not purged)
     * @param key the key
     * @return the KvEntry object or null if not found.
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    Future<KeyValueEntry> get(String key);

    /**
     * Get the specific revision of an entry for a key
     * when the key exists and is live (not deleted and not purged)
     * @param key the key
     * @param revision the revision
     * @return the KvEntry object or null if not found.
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    Future<KeyValueEntry> get(String key, long revision);

    /**
     * Put a byte[] as the value for a key
     * @param key the key
     * @param value the bytes of the value
     * @return the revision number for the key
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    Future<Long> put(String key, byte[] value);

    /**
     * Put a string as the value for a key
     * @param key the key
     * @param value the UTF-8 string
     * @return the revision number for the key
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    Future<Long> put(String key, String value);

    /**
     * Put a long as the value for a key
     * @param key the key
     * @param value the number
     * @return the revision number for the key
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    Future<Long> put(String key, Number value);

    /**
     * Put as the value for a key iff the key does not exist (there is no history)
     * or is deleted (history shows the key is deleted)
     * @param key the key
     * @param value the bytes of the value
     * @return the revision number for the key
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    Future<Long> create(String key, byte[] value);

    /**
     * Put as the value for a key iff the key exists and its last revision matches the expected
     * @param key the key
     * @param value the bytes of the value
     * @param expectedRevision the expected last revision
     * @return the revision number for the key
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    Future<Long> update(String key, byte[] value, long expectedRevision);

    /**
     * Put a string as the value for a key iff the key exists and its last revision matches the expected
     * @param key the key
     * @param value the UTF-8 string
     * @param expectedRevision the expected last revision
     * @return the revision number for the key
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    Future<Long> update(String key, String value, long expectedRevision);

    /**
     * Soft deletes the key by placing a delete marker.
     * @param key the key
     */
    Future<Void> delete(String key);

    /**
     * Soft deletes the key by placing a delete marker iff the key exists and its last revision matches the expected
     * @param key the key
     * @param expectedRevision the expected last revision
     */
    Future<Void> delete(String key, long expectedRevision);

    /**
     * Purge all values/history from the specific key
     * @param key the key
     */
    Future<Void> purge(String key);

    /**
     * Purge all values/history from the specific key iff the key exists and its last revision matches the expected
     * @param key the key
     * @param expectedRevision the expected last revision
     */
    Future<Void> purge(String key, long expectedRevision);

    /**
     * Watch updates for a specific key.
     * @param key the key.
     * @param watcher the watcher the implementation to receive changes
     * @param watchOptions the watch options to apply. If multiple conflicting options are supplied, the last options wins.
     * @return The KeyValueWatchSubscription
     */
    Future<NatsKeyValueWatchSubscription> watch(String key, KeyValueWatcher watcher, KeyValueWatchOption... watchOptions);

    /**
     * Watch updates for a specific key, starting at a specific revision.
     * @param key the key.
     * @param watcher the watcher the implementation to receive changes
     * @param fromRevision the revision to start from
     * @param watchOptions the watch options to apply. If multiple conflicting options are supplied, the last options wins.
     * @return The KeyValueWatchSubscription
     */
    Future<NatsKeyValueWatchSubscription> watch(String key, KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions);

    /**
     * Watch updates for specific keys.
     * @param keys the keys
     * @param watcher the watcher the implementation to receive changes
     * @param watchOptions the watch options to apply. If multiple conflicting options are supplied, the last options wins.
     * @return The KeyValueWatchSubscription
     */
    Future<NatsKeyValueWatchSubscription> watch(List<String> keys, KeyValueWatcher watcher, KeyValueWatchOption... watchOptions);

    /**
     * Watch updates for specific keys, starting at a specific revision.
     * @param keys the keys
     * @param watcher the watcher the implementation to receive changes
     * @param fromRevision the revision to start from
     * @param watchOptions the watch options to apply. If multiple conflicting options are supplied, the last options wins.
     * @return The KeyValueWatchSubscription
     */
    Future<NatsKeyValueWatchSubscription> watch(List<String> keys, KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions);

    /**
     * Watch updates for all keys.
     * @param watcher the watcher the implementation to receive changes
     * @param watchOptions the watch options to apply. If multiple conflicting options are supplied, the last options wins.
     * @return The KeyValueWatchSubscription
     */
    Future<NatsKeyValueWatchSubscription> watchAll(KeyValueWatcher watcher, KeyValueWatchOption... watchOptions);

    /**
     * Watch updates for all keys starting from a specific revision
     * @param watcher the watcher the implementation to receive changes
     * @param fromRevision the revision to start from
     * @param watchOptions the watch options to apply. If multiple conflicting options are supplied, the last options wins.
     * @return The KeyValueWatchSubscription
     */
    Future<NatsKeyValueWatchSubscription> watchAll(KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions);

    /**
     * Get a list of the keys in a bucket.
     * @return List of keys
     */
    Future<List<String>> keys();

    /**
     * Get a list of the keys in a bucket filtered by a
     * subject-like string, for instance "key" or "key.foo.*" or "key.&gt;"
     * @param filter the subject like key filter
     * @return List of keys
     */
    Future<List<String>> keys(String filter);

    /**
     * Get a list of the keys in a bucket filtered by
     * subject-like strings, for instance "aaa.*", "bbb.*;"
     * @param filters the subject like key filters
     * @return List of keys
     */
    Future<List<String>> keys(List<String> filters);

    /**
     * Get the history (list of KeyValueEntry) for a key
     * @param key the key
     * @return List of KvEntry
     */
    Future<List<KeyValueEntry>> history(String key);

    /**
     * Remove history from all keys that currently are deleted or purged
     * with using a default KeyValuePurgeOptions
     */
    Future<Void> purgeDeletes();

    /**
     * Remove history from all keys that currently are deleted or purged, considering options.
     * @param options the purge options
     */
    Future<Void> purgeDeletes(KeyValuePurgeOptions options);

    /**
     * Get the KeyValueStatus object
     * @return the status object
     */
    Future<KeyValueStatus> getStatus();

    /**
     * Get access to the underlying implementation which contains
     * the connection, the JetStream context, and other components
     * @return the NatsImp object
     */
    NatsImpl getImpl();

    String readSubject(String key);

    String writeSubject(String key);
}
