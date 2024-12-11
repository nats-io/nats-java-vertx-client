package io.nats.vertx;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.NatsKeyValueWatchSubscription;
import io.nats.client.support.NatsKeyValueUtil;
import io.vertx.core.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.nats.client.api.KeyValueWatchOption.*;
import static io.nats.vertx.TestUtils2.sleep;
import static io.nats.vertx.TestUtils2.unique;
import static org.junit.jupiter.api.Assertions.*;

public class NatsVertxKeyValueTest {

    // ----------------------------------------------------------------------------------------------------
    // Test Running
    // ----------------------------------------------------------------------------------------------------
    static TestsRunner testRunner;
    static int port;

    @AfterAll
    public static void afterAll() throws Exception {
        testRunner.close();
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        testRunner = TestsRunner.instance();
        port = testRunner.natsServerRunner.getPort();
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        cleanupJs();
    }

    private static void cleanupJs() {
        try {
            JetStreamManagement jsm = testRunner.nc.jetStreamManagement();
            List<String> streams = jsm.getStreamNames();
            for (String s : streams)
            {
                jsm.deleteStream(s);
            }
        } catch (Exception ignore) {}
    }

    private static NatsClient getNatsClient() {
        return TestUtils2.natsClient(port);
    }

    // ----------------------------------------------------------------------------------------------------

    @Test
    public void testWorkflow() throws Exception {
        long now = ZonedDateTime.now().toEpochSecond();

        String byteKey = "key.byte" + unique();
        String stringKey = "key.string" + unique();
        String longKey = "key.long" + unique();
        String notFoundKey = "notFound" + unique();
        String byteValue1 = "Byte Value 1";
        String byteValue2 = "Byte Value 2";
        String stringValue1 = "String Value 1";
        String stringValue2 = "String Value 2";

        String desc = "desc" + unique();
        KvTester tester = new KvTester(b -> b.description(desc).maxHistoryPerKey(3));
        //noinspection DataFlowIssue
        assertInitialStatus(tester.status, tester.bucket, desc);

        // get the kv context for the specific bucket

        assertEquals(tester.bucket, tester.kv.getBucketName());
        KeyValueStatus status = tester.getStatus();
        assertInitialStatus(status, tester.bucket, desc);

        // Put some keys. Each key is put in a subject in the bucket (stream)
        // The put returns the sequence number in the bucket (stream)
        assertEquals(1, tester.put(byteKey, byteValue1.getBytes()));
        assertEquals(2, tester.put(stringKey, stringValue1));
        assertEquals(3, tester.put(longKey, 1));

        // retrieve the values. all types are stored as bytes
        // so you can always get the bytes directly
        assertEquals(byteValue1, new String(tester.get(byteKey).getValue()));
        assertEquals(stringValue1, new String(tester.get(stringKey).getValue()));
        assertEquals("1", new String(tester.get(longKey).getValue()));

        // if you know the value is not binary and can safely be read
        // as a UTF-8 string, the getStringValue method is ok to use
        assertEquals(byteValue1, tester.get(byteKey).getValueAsString());
        assertEquals(stringValue1, tester.get(stringKey).getValueAsString());
        assertEquals("1", tester.get(longKey).getValueAsString());

        // if you know the value is a long, you can use
        // the getLongValue method
        // if it's not a number a NumberFormatException is thrown
        assertEquals(1, tester.get(longKey).getValueAsLong());
        assertThrows(NumberFormatException.class, () -> tester.get(stringKey).getValueAsLong());

        // going to manually track history for verification later
        List<Object> byteHistory = new ArrayList<>();
        List<Object> stringHistory = new ArrayList<>();
        List<Object> longHistory = new ArrayList<>();

        // entry gives detail about the latest entry of the key
        byteHistory.add(
            assertEntry(tester.bucket, byteKey, KeyValueOperation.PUT, 1, byteValue1, now, tester.get(byteKey)));

        stringHistory.add(
            assertEntry(tester.bucket, stringKey, KeyValueOperation.PUT, 2, stringValue1, now, tester.get(stringKey)));

        longHistory.add(
            assertEntry(tester.bucket, longKey, KeyValueOperation.PUT, 3, "1", now, tester.get(longKey)));

        // history gives detail about the key
        assertHistory(byteHistory, tester.history(byteKey));
        assertHistory(stringHistory, tester.history(stringKey));
        assertHistory(longHistory, tester.history(longKey));

        // let's check the bucket info
        status = tester.kvm.getStatus(tester.bucket);
        assertState(status, 3, 3);

        // delete a key. Its entry will still exist, but its value is null
        tester.delete(byteKey);
        assertNull(tester.get(byteKey));
        byteHistory.add(KeyValueOperation.DELETE);
        assertHistory(byteHistory, tester.history(byteKey));

        // hashCode coverage
        assertEquals(byteHistory.get(0).hashCode(), byteHistory.get(0).hashCode());
        assertNotEquals(byteHistory.get(0).hashCode(), byteHistory.get(1).hashCode());

        // let's check the bucket info
        status = tester.kvm.getStatus(tester.bucket);
        assertState(status, 4, 4);

        // if the key has been deleted no entry is returned
        assertNull(tester.get(byteKey));

        // if the key does not exist (no history) there is no entry
        assertNull(tester.get(notFoundKey));

        // Update values. You can even update a deleted key
        assertEquals(5, tester.put(byteKey, byteValue2.getBytes()));
        assertEquals(6, tester.put(stringKey, stringValue2));
        assertEquals(7, tester.put(longKey, 2));

        // values after updates
        assertEquals(byteValue2, new String(tester.get(byteKey).getValue()));
        assertEquals(stringValue2, tester.get(stringKey).getValueAsString());
        assertEquals(2, tester.get(longKey).getValueAsLong());

        // entry and history after update
        byteHistory.add(
            assertEntry(tester.bucket, byteKey, KeyValueOperation.PUT, 5, byteValue2, now, tester.get(byteKey)));
        assertHistory(byteHistory, tester.history(byteKey));

        stringHistory.add(
            assertEntry(tester.bucket, stringKey, KeyValueOperation.PUT, 6, stringValue2, now, tester.get(stringKey)));
        assertHistory(stringHistory, tester.history(stringKey));

        longHistory.add(
            assertEntry(tester.bucket, longKey, KeyValueOperation.PUT, 7, "2", now, tester.get(longKey)));
        assertHistory(longHistory, tester.history(longKey));

        // let's check the bucket info
        status = tester.kvm.getStatus(tester.bucket);
        assertState(status, 7, 7);

        // make sure it only keeps the correct amount of history
        assertEquals(8, tester.put(longKey, 3));
        assertEquals(3, tester.get(longKey).getValueAsLong());

        longHistory.add(
            assertEntry(tester.bucket, longKey, KeyValueOperation.PUT, 8, "3", now, tester.get(longKey)));
        assertHistory(longHistory, tester.history(longKey));

        status = tester.kvm.getStatus(tester.bucket);
        assertState(status, 8, 8);

        // this would be the 4th entry for the longKey
        // sp the total records will stay the same
        assertEquals(9, tester.put(longKey, 4));
        assertEquals(4, tester.get(longKey).getValueAsLong());

        // history only retains 3 records
        longHistory.remove(0);
        longHistory.add(
            assertEntry(tester.bucket, longKey, KeyValueOperation.PUT, 9, "4", now, tester.get(longKey)));
        assertHistory(longHistory, tester.history(longKey));

        // record count does not increase
        status = tester.kvm.getStatus(tester.bucket);
        assertState(status, 8, 9);

        assertKeys(tester.keys(), byteKey, stringKey, longKey);
        assertKeys(tester.keys("key.>"), byteKey, stringKey, longKey);
        assertKeys(tester.keys(byteKey), byteKey);
        assertKeys(tester.keys(Arrays.asList(longKey, stringKey)), longKey, stringKey);

        // purge
        tester.purge(longKey);
        longHistory.clear();
        assertNull(tester.get(longKey));
        longHistory.add(KeyValueOperation.PURGE);
        assertHistory(longHistory, tester.history(longKey));

        status = tester.kvm.getStatus(tester.bucket);
        assertState(status, 6, 10);

        // only 2 keys now
        assertKeys(tester.keys(), byteKey, stringKey);

        tester.purge(byteKey);
        byteHistory.clear();
        assertNull(tester.get(byteKey));
        byteHistory.add(KeyValueOperation.PURGE);
        assertHistory(byteHistory, tester.history(byteKey));

        status = tester.kvm.getStatus(tester.bucket);
        assertState(status, 4, 11);

        // only 1 key now
        assertKeys(tester.keys(), stringKey);

        tester.purge(stringKey);
        stringHistory.clear();
        assertNull(tester.get(stringKey));
        stringHistory.add(KeyValueOperation.PURGE);
        assertHistory(stringHistory, tester.history(stringKey));

        status = tester.kvm.getStatus(tester.bucket);
        assertState(status, 3, 12);

        // no more keys left
        assertKeys(tester.keys());

        // clear things
        KeyValuePurgeOptions kvpo = KeyValuePurgeOptions.builder().deleteMarkersNoThreshold().build();
        tester.purgeDeletes(kvpo);
        status = tester.kvm.getStatus(tester.bucket);
        assertState(status, 0, 12);

        longHistory.clear();
        assertHistory(longHistory, tester.history(longKey));

        stringHistory.clear();
        assertHistory(stringHistory, tester.history(stringKey));

        // put some more
        assertEquals(13, tester.put(longKey, 110));
        longHistory.add(
            assertEntry(tester.bucket, longKey, KeyValueOperation.PUT, 13, "110", now, tester.get(longKey)));

        assertEquals(14, tester.put(longKey, 111));
        longHistory.add(
            assertEntry(tester.bucket, longKey, KeyValueOperation.PUT, 14, "111", now, tester.get(longKey)));

        assertEquals(15, tester.put(longKey, 112));
        longHistory.add(
            assertEntry(tester.bucket, longKey, KeyValueOperation.PUT, 15, "112", now, tester.get(longKey)));

        assertEquals(16, tester.put(stringKey, stringValue1));
        stringHistory.add(
            assertEntry(tester.bucket, stringKey, KeyValueOperation.PUT, 16, stringValue1, now, tester.get(stringKey)));

        assertEquals(17, tester.put(stringKey, stringValue2));
        stringHistory.add(
            assertEntry(tester.bucket, stringKey, KeyValueOperation.PUT, 17, stringValue2, now, tester.get(stringKey)));

        assertHistory(longHistory, tester.history(longKey));
        assertHistory(stringHistory, tester.history(stringKey));

        status = tester.kvm.getStatus(tester.bucket);
        assertState(status, 5, 17);

        // delete the bucket
        tester.kvm.delete(tester.bucket);
        assertThrows(JetStreamApiException.class, () -> tester.kvm.delete(tester.bucket));
        assertThrows(JetStreamApiException.class, () -> tester.kvm.getStatus(tester.bucket));

        assertEquals(0, tester.kvm.getBucketNames().size());
    }

    private static void assertState(KeyValueStatus status, int entryCount, int lastSeq) {
        assertEquals(entryCount, status.getEntryCount());
        assertEquals(lastSeq, status.getBackingStreamInfo().getStreamState().getLastSequence());
        assertEquals(status.getByteCount(), status.getBackingStreamInfo().getStreamState().getByteCount());
    }

    private void assertInitialStatus(KeyValueStatus status, String bucket, String desc) {
        KeyValueConfiguration kvc = status.getConfiguration();
        assertEquals(bucket, status.getBucketName());
        assertEquals(bucket, kvc.getBucketName());
        assertEquals(desc, status.getDescription());
        assertEquals(desc, kvc.getDescription());
        assertEquals(NatsKeyValueUtil.toStreamName(bucket), kvc.getBackingConfig().getName());
        assertEquals(3, status.getMaxHistoryPerKey());
        assertEquals(3, kvc.getMaxHistoryPerKey());
        assertEquals(-1, status.getMaxBucketSize());
        assertEquals(-1, kvc.getMaxBucketSize());
        assertEquals(-1, status.getMaximumValueSize());
        assertEquals(-1, kvc.getMaximumValueSize());
        assertEquals(Duration.ZERO, status.getTtl());
        assertEquals(Duration.ZERO, kvc.getTtl());
        assertEquals(StorageType.Memory, status.getStorageType());
        assertEquals(StorageType.Memory, kvc.getStorageType());
        assertNull(status.getPlacement());
        assertNull(status.getRepublish());
        assertEquals(1, status.getReplicas());
        assertEquals(1, kvc.getReplicas());
        assertEquals(0, status.getEntryCount());
        assertEquals("JetStream", status.getBackingStore());
        assertNotNull(status.getConfiguration()); // coverage
        assertNotNull(status.getConfiguration().toString()); // coverage
        assertNotNull(status.toString()); // coverage
        assertTrue(status.toString().contains(bucket));
        assertTrue(status.toString().contains(desc));
    }

    @Test
    public void testGetRevision() throws Exception {
        KvTester tester = new KvTester(b -> b.maxHistoryPerKey(2));

        String key = unique();

        long seq1 = tester.put(key, 1);
        long seq2 = tester.put(key, 2);
        long seq3 = tester.put(key, 3);

        KeyValueEntry kve = tester.get(key);
        assertNotNull(kve);
        assertEquals(3, kve.getValueAsLong());

        kve = tester.get(key, seq3);
        assertNotNull(kve);
        assertEquals(3, kve.getValueAsLong());

        kve = tester.get(key, seq2);
        assertNotNull(kve);
        assertEquals(2, kve.getValueAsLong());

        kve = tester.get(key, seq1);
        assertNull(kve);

        kve = tester.get("notkey", seq3);
        assertNull(kve);
    }

    @Test
    public void testKeys() throws Exception {
        KvTester tester = new KvTester();

        for (int x = 1; x <= 10; x++) {
            tester.put("k" + x, x);
        }

        List<String> keys = tester.keys();
        assertEquals(10, keys.size());
        for (int x = 1; x <= 10; x++) {
            assertTrue(keys.contains("k" + x));
        }
    }

    @Test
    public void testMaxHistoryPerKey() throws Exception {
        KvTester tester1 = new KvTester();

        String key = unique();
        tester1.put(key, 1);
        tester1.put(key, 2);

        List<KeyValueEntry> history = tester1.history(key);
        assertEquals(1, history.size());
        assertEquals(2, history.get(0).getValueAsLong());

        KvTester tester2 = new KvTester(b -> b.maxHistoryPerKey(2));

        key = unique();
        tester2.put(key, 1);
        tester2.put(key, 2);
        tester2.put(key, 3);

        history = tester2.history(key);
        assertEquals(2, history.size());
        assertEquals(2, history.get(0).getValueAsLong());
        assertEquals(3, history.get(1).getValueAsLong());
    }

    @Test
    public void testCreateUpdate() throws Exception {
        KvTester tester = new KvTester();

        //noinspection DataFlowIssue
        assertEquals(tester.bucket, tester.status.getBucketName());
        assertNull(tester.status.getDescription());
        assertEquals(1, tester.status.getMaxHistoryPerKey());
        assertEquals(-1, tester.status.getMaxBucketSize());
        assertEquals(-1, tester.status.getMaximumValueSize());
        assertEquals(Duration.ZERO, tester.status.getTtl());
        assertEquals(StorageType.Memory, tester.status.getStorageType());
        assertEquals(1, tester.status.getReplicas());
        assertEquals(0, tester.status.getEntryCount());
        assertEquals("JetStream", tester.status.getBackingStore());

        String key = unique();
        tester.put(key, 1);
        tester.put(key, 2);

        List<KeyValueEntry> history = tester.history(key);
        assertEquals(1, history.size());
        assertEquals(2, history.get(0).getValueAsLong());

        boolean compression = true;
        String desc = unique();
        KeyValueConfiguration kvc = KeyValueConfiguration.builder(tester.status.getConfiguration())
            .description(desc)
            .maxHistoryPerKey(3)
            .maxBucketSize(10_000)
            .maximumValueSize(100)
            .ttl(Duration.ofHours(1))
            .compression(compression)
            .build();

        KeyValueStatus kvs = tester.kvm.update(kvc);

        assertEquals(tester.bucket, kvs.getBucketName());
        assertEquals(desc, kvs.getDescription());
        assertEquals(3, kvs.getMaxHistoryPerKey());
        assertEquals(10_000, kvs.getMaxBucketSize());
        assertEquals(100, kvs.getMaximumValueSize());
        assertEquals(Duration.ofHours(1), kvs.getTtl());
        assertEquals(StorageType.Memory, kvs.getStorageType());
        assertEquals(1, kvs.getReplicas());
        assertEquals(1, kvs.getEntryCount());
        assertEquals("JetStream", kvs.getBackingStore());
        assertEquals(compression, kvs.isCompressed());

        history = tester.history(key);
        assertEquals(1, history.size());
        assertEquals(2, history.get(0).getValueAsLong());

        KeyValueConfiguration kvcStor = KeyValueConfiguration.builder(kvs.getConfiguration())
            .storageType(StorageType.File)
            .build();
        assertThrows(JetStreamApiException.class, () -> tester.kvm.update(kvcStor));
    }

    @Test
    public void testHistoryDeletePurge() throws Exception {
        KvTester tester = new KvTester(b -> b.maxHistoryPerKey(64));

        String key = unique();
        tester.put(key, "a");
        tester.put(key, "b");
        tester.put(key, "c");
        List<KeyValueEntry> list = tester.history(key);
        assertEquals(3, list.size());

        tester.delete(key);
        list = tester.history(key);
        assertEquals(4, list.size());

        tester.purge(key);
        list = tester.history(key);
        assertEquals(1, list.size());
    }

    @Test
    public void testAtomicDeleteAtomicPurge() throws Exception {
        KvTester tester = new KvTester(b -> b.maxHistoryPerKey(64));

        String key = unique();
        tester.put(key, "a");
        tester.put(key, "b");
        tester.put(key, "c");
        assertEquals(3, tester.get(key).getRevision());

        // Delete wrong revision rejected
        tester.delete(key, 1);
        tester.assertThrew(JetStreamApiException.class);

        // Correct revision writes tombstone and bumps revision
        tester.delete(key, 3);

        assertHistory(Arrays.asList(
                tester.get(key, 1L),
                tester.get(key, 2L),
                tester.get(key, 3L),
                KeyValueOperation.DELETE),
            tester.history(key));

        // Wrong revision rejected again
        tester.delete(key, 3);
        tester.assertThrew(JetStreamApiException.class);

        // Delete is idempotent: two consecutive tombstones
        tester.delete(key, 4);

        assertHistory(Arrays.asList(
                tester.get(key, 1L),
                tester.get(key, 2L),
                tester.get(key, 3L),
                KeyValueOperation.DELETE,
                KeyValueOperation.DELETE),
            tester.history(key));

        // Purge wrong revision rejected
        tester.purge(key, 1);
        tester.assertThrew(JetStreamApiException.class);

        // Correct revision writes roll-up purge tombstone
        tester.purge(key, 5);

        assertHistory(Arrays.asList(KeyValueOperation.PURGE), tester.history(key));
    }

    @Test
    public void testPurgeDeletes() throws Exception {
        KvTester tester = new KvTester(b -> b.maxHistoryPerKey(64));

        String key1 = unique();
        String key2 = unique();
        String key3 = unique();
        String key4 = unique();
        tester.put(key1, "a");
        tester.delete(key1);
        tester.put(key2, "b");
        tester.put(key3, "c");
        tester.put(key4, "d");
        tester.purge(key4);

        JetStream js = testRunner.nc.jetStream();

        assertPurgeDeleteEntries(js, tester.bucket, new String[]{"a", null, "b", "c", null});

        // default purge deletes uses the default threshold
        // so no markers will be deleted
        tester.purgeDeletes();
        assertPurgeDeleteEntries(js, tester.bucket, new String[]{null, "b", "c", null});

        // deleteMarkersThreshold of 0 the default threshold
        // so no markers will be deleted
        tester.purgeDeletes(KeyValuePurgeOptions.builder().deleteMarkersThreshold(0).build());
        assertPurgeDeleteEntries(js, tester.bucket, new String[]{null, "b", "c", null});

        // no threshold causes all to be removed
        tester.purgeDeletes(KeyValuePurgeOptions.builder().deleteMarkersNoThreshold().build());
        assertPurgeDeleteEntries(js, tester.bucket, new String[]{"b", "c"});
    }

    private void assertPurgeDeleteEntries(JetStream js, String bucket, String[] expected) throws IOException, JetStreamApiException, InterruptedException {
        JetStreamSubscription sub = js.subscribe(NatsKeyValueUtil.toStreamSubject(bucket));

        for (String s : expected) {
            Message m = sub.nextMessage(1000);
            KeyValueEntry kve = new KeyValueEntry(m);
            if (s == null) {
                assertNotEquals(KeyValueOperation.PUT, kve.getOperation());
                assertEquals(0, kve.getDataLen());
            }
            else {
                assertEquals(KeyValueOperation.PUT, kve.getOperation());
                assertEquals(s, kve.getValueAsString());
            }
        }

        sub.unsubscribe();
    }

    @Test
    public void testCreateAndUpdate() throws Exception {
        KvTester tester = new KvTester(b -> b.maxHistoryPerKey(64));

        String key = unique();
        // 1. allowed to create something that does not exist
        long rev1 = tester.create(key, "a".getBytes());

        // 2. allowed to update with proper revision
        tester.update(key, "ab".getBytes(), rev1);

        // 3. not allowed to update with wrong revision
        tester.update(key, "zzz".getBytes(), rev1);
        tester.assertThrew(JetStreamApiException.class);

        // 4. not allowed to create a key that exists
        tester.create(key, "zzz".getBytes());
        tester.assertThrew(JetStreamApiException.class);

        // 5. not allowed to update a key that does not exist
        tester.update(key, "zzz".getBytes(), 1);
        tester.assertThrew(JetStreamApiException.class);

        // 6. allowed to create a key that is deleted
        tester.delete(key);
        tester.create(key, "abc".getBytes());

        // 7. allowed to update a key that is deleted, as long as you have it's revision
        tester.delete(key);
        testRunner.nc.flush(Duration.ofSeconds(1));

        sleep(200); // a little pause to make sure things get flushed
        List<KeyValueEntry> hist = tester.history(key);
        tester.update(key, "abcd".getBytes(), hist.get(hist.size() - 1).getRevision());

        // 8. allowed to create a key that is purged
        tester.purge(key);
        tester.create(key, "abcde".getBytes());

        // 9. allowed to update a key that is deleted, as long as you have its revision
        tester.purge(key);

        sleep(200); // a little pause to make sure things get flushed
        hist = tester.history(key);
        tester.update(key, "abcdef".getBytes(), hist.get(hist.size() - 1).getRevision());
    }
    
    private void assertKeys(List<String> apiKeys, String... manualKeys) {
        assertEquals(manualKeys.length, apiKeys.size());
        for (String k : manualKeys) {
            assertTrue(apiKeys.contains(k));
        }
    }

    private void assertHistory(List<Object> manualHistory, List<KeyValueEntry> apiHistory) {
        assertEquals(apiHistory.size(), manualHistory.size());
        for (int x = 0; x < apiHistory.size(); x++) {
            Object o = manualHistory.get(x);
            if (o instanceof KeyValueOperation) {
                assertEquals((KeyValueOperation)o, apiHistory.get(x).getOperation());
            }
            else {
                assertKvEquals((KeyValueEntry)o, apiHistory.get(x));
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    private KeyValueEntry assertEntry(String bucket, String key, KeyValueOperation op, long seq, String value, long now, KeyValueEntry entry) {
        assertEquals(bucket, entry.getBucket());
        assertEquals(key, entry.getKey());
        assertEquals(op, entry.getOperation());
        assertEquals(seq, entry.getRevision());
        assertEquals(0, entry.getDelta());
        if (op == KeyValueOperation.PUT) {
            assertEquals(value, new String(entry.getValue()));
        }
        else {
            assertNull(entry.getValue());
        }
        assertTrue(now <= entry.getCreated().toEpochSecond());

        // coverage
        assertNotNull(entry.toString());
        return entry;
    }

    private void assertKvEquals(KeyValueEntry kv1, KeyValueEntry kv2) {
        assertEquals(kv1.getOperation(), kv2.getOperation());
        assertEquals(kv1.getRevision(), kv2.getRevision());
        assertEquals(kv1.getKey(), kv2.getKey());
        assertEquals(kv1.getKey(), kv2.getKey());
        assertArrayEquals(kv1.getValue(), kv2.getValue());
        long es1 = kv1.getCreated().toEpochSecond();
        long es2 = kv2.getCreated().toEpochSecond();
        assertEquals(es1, es2);
    }

    static class TestKeyValueWatcher implements KeyValueWatcher {
        public String name;
        public List<KeyValueEntry> entries = new ArrayList<>();
        public KeyValueWatchOption[] watchOptions;
        public boolean beforeWatcher;
        public boolean metaOnly;
        public int endOfDataReceived;
        public boolean endBeforeEntries;

        public TestKeyValueWatcher(String name, boolean beforeWatcher, KeyValueWatchOption... watchOptions) {
            this.name = name;
            this.beforeWatcher = beforeWatcher;
            this.watchOptions = watchOptions;
            for (KeyValueWatchOption wo : watchOptions) {
                if (wo == META_ONLY) {
                    metaOnly = true;
                    break;
                }
            }
        }

        @Override
        public String toString() {
            return "TestKeyValueWatcher{" +
                "name='" + name + '\'' +
                ", beforeWatcher=" + beforeWatcher +
                ", metaOnly=" + metaOnly +
                ", watchOptions=" + Arrays.toString(watchOptions) +
                '}';
        }

        @Override
        public void watch(KeyValueEntry kve) {
            entries.add(kve);
        }

        @Override
        public void endOfData() {
            if (++endOfDataReceived == 1 && entries.isEmpty()) {
                endBeforeEntries = true;
            }
        }
    }

    static String TEST_WATCH_KEY_NULL = "key.nl";
    static String TEST_WATCH_KEY_1 = "key.1";
    static String TEST_WATCH_KEY_2 = "key.2";

    interface  TestWatchSubSupplier {
        NatsKeyValueWatchSubscription get(KvTester tester) throws Exception;
    }

    @Test
    public void testWatch() throws Exception {
        Object[] key1AllExpecteds = new Object[]{
            "a", "aa", KeyValueOperation.DELETE, "aaa", KeyValueOperation.DELETE, KeyValueOperation.PURGE
        };

        Object[] key1FromRevisionExpecteds = new Object[]{
            "aa", KeyValueOperation.DELETE, "aaa"
        };

        Object[] noExpecteds = new Object[0];
        Object[] purgeOnlyExpecteds = new Object[]{KeyValueOperation.PURGE};

        Object[] key2AllExpecteds = new Object[]{
            "z", "zz", KeyValueOperation.DELETE, "zzz"
        };

        Object[] key2AfterExpecteds = new Object[]{"zzz"};

        Object[] allExpecteds = new Object[]{
            "a", "aa", "z", "zz",
            KeyValueOperation.DELETE, KeyValueOperation.DELETE,
            "aaa", "zzz",
            KeyValueOperation.DELETE, KeyValueOperation.PURGE,
            null
        };

        Object[] allPutsExpecteds = new Object[]{
            "a", "aa", "z", "zz", "aaa", "zzz", null
        };

        Object[] allFromRevisionExpecteds = new Object[]{
            "aa", "z", "zz",
            KeyValueOperation.DELETE, KeyValueOperation.DELETE,
            "aaa", "zzz",
        };

        TestKeyValueWatcher key1FullWatcher = new TestKeyValueWatcher("key1FullWatcher", true);
        TestKeyValueWatcher key1MetaWatcher = new TestKeyValueWatcher("key1MetaWatcher", true, META_ONLY);
        TestKeyValueWatcher key1StartNewWatcher = new TestKeyValueWatcher("key1StartNewWatcher", true, META_ONLY, UPDATES_ONLY);
        TestKeyValueWatcher key1StartAllWatcher = new TestKeyValueWatcher("key1StartAllWatcher", true, META_ONLY);
        TestKeyValueWatcher key2FullWatcher = new TestKeyValueWatcher("key2FullWatcher", true);
        TestKeyValueWatcher key2MetaWatcher = new TestKeyValueWatcher("key2MetaWatcher", true, META_ONLY);
        TestKeyValueWatcher allAllFullWatcher = new TestKeyValueWatcher("allAllFullWatcher", true);
        TestKeyValueWatcher allAllMetaWatcher = new TestKeyValueWatcher("allAllMetaWatcher", true, META_ONLY);
        TestKeyValueWatcher allIgDelFullWatcher = new TestKeyValueWatcher("allIgDelFullWatcher", true, IGNORE_DELETE);
        TestKeyValueWatcher allIgDelMetaWatcher = new TestKeyValueWatcher("allIgDelMetaWatcher", true, META_ONLY, IGNORE_DELETE);
        TestKeyValueWatcher starFullWatcher = new TestKeyValueWatcher("starFullWatcher", true);
        TestKeyValueWatcher starMetaWatcher = new TestKeyValueWatcher("starMetaWatcher", true, META_ONLY);
        TestKeyValueWatcher gtFullWatcher = new TestKeyValueWatcher("gtFullWatcher", true);
        TestKeyValueWatcher gtMetaWatcher = new TestKeyValueWatcher("gtMetaWatcher", true, META_ONLY);
        TestKeyValueWatcher multipleFullWatcher = new TestKeyValueWatcher("multipleFullWatcher", true);
        TestKeyValueWatcher multipleMetaWatcher = new TestKeyValueWatcher("multipleMetaWatcher", true, META_ONLY);
        TestKeyValueWatcher key1AfterWatcher = new TestKeyValueWatcher("key1AfterWatcher", false, META_ONLY);
        TestKeyValueWatcher key1AfterIgDelWatcher = new TestKeyValueWatcher("key1AfterIgDelWatcher", false, META_ONLY, IGNORE_DELETE);
        TestKeyValueWatcher key1AfterStartNewWatcher = new TestKeyValueWatcher("key1AfterStartNewWatcher", false, META_ONLY, UPDATES_ONLY);
        TestKeyValueWatcher key1AfterStartFirstWatcher = new TestKeyValueWatcher("key1AfterStartFirstWatcher", false, META_ONLY, INCLUDE_HISTORY);
        TestKeyValueWatcher key2AfterWatcher = new TestKeyValueWatcher("key2AfterWatcher", false, META_ONLY);
        TestKeyValueWatcher key2AfterStartNewWatcher = new TestKeyValueWatcher("key2AfterStartNewWatcher", false, META_ONLY, UPDATES_ONLY);
        TestKeyValueWatcher key2AfterStartFirstWatcher = new TestKeyValueWatcher("key2AfterStartFirstWatcher", false, META_ONLY, INCLUDE_HISTORY);
        TestKeyValueWatcher key1FromRevisionAfterWatcher = new TestKeyValueWatcher("key1FromRevisionAfterWatcher", false);
        TestKeyValueWatcher allFromRevisionAfterWatcher = new TestKeyValueWatcher("allFromRevisionAfterWatcher", false);

        List<String> allKeys = Arrays.asList(TEST_WATCH_KEY_1, TEST_WATCH_KEY_2, TEST_WATCH_KEY_NULL);

        _testWatch(key1FullWatcher, key1AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1FullWatcher, key1FullWatcher.watchOptions));
        _testWatch(key1MetaWatcher, key1AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1MetaWatcher, key1MetaWatcher.watchOptions));
        _testWatch(key1StartNewWatcher, key1AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1StartNewWatcher, key1StartNewWatcher.watchOptions));
        _testWatch(key1StartAllWatcher, key1AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1StartAllWatcher, key1StartAllWatcher.watchOptions));
        _testWatch(key2FullWatcher, key2AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_2, key2FullWatcher, key2FullWatcher.watchOptions));
        _testWatch(key2MetaWatcher, key2AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_2, key2MetaWatcher, key2MetaWatcher.watchOptions));
        _testWatch(allAllFullWatcher, allExpecteds, -1, kv -> kv.watchAll(allAllFullWatcher, allAllFullWatcher.watchOptions));
        _testWatch(allAllMetaWatcher, allExpecteds, -1, kv -> kv.watchAll(allAllMetaWatcher, allAllMetaWatcher.watchOptions));
        _testWatch(allIgDelFullWatcher, allPutsExpecteds, -1, kv -> kv.watchAll(allIgDelFullWatcher, allIgDelFullWatcher.watchOptions));
        _testWatch(allIgDelMetaWatcher, allPutsExpecteds, -1, kv -> kv.watchAll(allIgDelMetaWatcher, allIgDelMetaWatcher.watchOptions));
        _testWatch(starFullWatcher, allExpecteds, -1, kv -> kv.watch("key.*", starFullWatcher, starFullWatcher.watchOptions));
        _testWatch(starMetaWatcher, allExpecteds, -1, kv -> kv.watch("key.*", starMetaWatcher, starMetaWatcher.watchOptions));
        _testWatch(gtFullWatcher, allExpecteds, -1, kv -> kv.watch("key.>", gtFullWatcher, gtFullWatcher.watchOptions));
        _testWatch(gtMetaWatcher, allExpecteds, -1, kv -> kv.watch("key.>", gtMetaWatcher, gtMetaWatcher.watchOptions));
        _testWatch(key1AfterWatcher, purgeOnlyExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1AfterWatcher, key1AfterWatcher.watchOptions));
        _testWatch(key1AfterIgDelWatcher, noExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1AfterIgDelWatcher, key1AfterIgDelWatcher.watchOptions));
        _testWatch(key1AfterStartNewWatcher, noExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1AfterStartNewWatcher, key1AfterStartNewWatcher.watchOptions));
        _testWatch(key1AfterStartFirstWatcher, purgeOnlyExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1AfterStartFirstWatcher, key1AfterStartFirstWatcher.watchOptions));
        _testWatch(key2AfterWatcher, key2AfterExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_2, key2AfterWatcher, key2AfterWatcher.watchOptions));
        _testWatch(key2AfterStartNewWatcher, noExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_2, key2AfterStartNewWatcher, key2AfterStartNewWatcher.watchOptions));
        _testWatch(key2AfterStartFirstWatcher, key2AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_2, key2AfterStartFirstWatcher, key2AfterStartFirstWatcher.watchOptions));
//        _testWatch(key1FromRevisionAfterWatcher, key1FromRevisionExpecteds, 2, kv -> kv.watch(TEST_WATCH_KEY_1, key1FromRevisionAfterWatcher, 2, key1FromRevisionAfterWatcher.watchOptions));
        _testWatch(allFromRevisionAfterWatcher, allFromRevisionExpecteds, 2, kv -> kv.watchAll(allFromRevisionAfterWatcher, 2, allFromRevisionAfterWatcher.watchOptions));

        _testWatch(multipleFullWatcher, allExpecteds, -1, kv -> kv.watch(allKeys, multipleFullWatcher, multipleFullWatcher.watchOptions));
        _testWatch(multipleMetaWatcher, allExpecteds, -1, kv -> kv.watch(allKeys, multipleMetaWatcher, multipleMetaWatcher.watchOptions));
    }

    private void _testWatch(TestKeyValueWatcher watcher, Object[] expectedKves, long fromRevision, TestWatchSubSupplier supplier) throws Exception {
        String bucket = unique() + watcher.name + "Bucket";
        KvTester tester = new KvTester(bucket, b -> b.maxHistoryPerKey(10));

        NatsKeyValueWatchSubscription sub = null;

        if (watcher.beforeWatcher) {
            sub = supplier.get(tester);
        }

        if (fromRevision == -1) {
            tester.put(TEST_WATCH_KEY_1, "a");
            tester.put(TEST_WATCH_KEY_1, "aa");
            tester.put(TEST_WATCH_KEY_2, "z");
            tester.put(TEST_WATCH_KEY_2, "zz");
            tester.delete(TEST_WATCH_KEY_1);
            tester.delete(TEST_WATCH_KEY_2);
            tester.put(TEST_WATCH_KEY_1, "aaa");
            tester.put(TEST_WATCH_KEY_2, "zzz");
            tester.delete(TEST_WATCH_KEY_1);
            tester.purge(TEST_WATCH_KEY_1);
            tester.put(TEST_WATCH_KEY_NULL, (byte[]) null);
        }
        else {
            tester.put(TEST_WATCH_KEY_1, "a");
            tester.put(TEST_WATCH_KEY_1, "aa");
            tester.put(TEST_WATCH_KEY_2, "z");
            tester.put(TEST_WATCH_KEY_2, "zz");
            tester.delete(TEST_WATCH_KEY_1);
            tester.delete(TEST_WATCH_KEY_2);
            tester.put(TEST_WATCH_KEY_1, "aaa");
            tester.put(TEST_WATCH_KEY_2, "zzz");
        }

        if (!watcher.beforeWatcher) {
            sub = supplier.get(tester);
        }

        sleep(1500); // give time for the watches to get messages

        validateWatcher(expectedKves, watcher);
        //noinspection ConstantConditions
        sub.unsubscribe();
        tester.kvm.delete(bucket);
    }

    private void validateWatcher(Object[] expectedKves, TestKeyValueWatcher watcher) {
        assertEquals(expectedKves.length, watcher.entries.size());
        assertEquals(1, watcher.endOfDataReceived);

        if (expectedKves.length > 0) {
            assertEquals(watcher.beforeWatcher, watcher.endBeforeEntries);
        }

        int aix = 0;
        ZonedDateTime lastCreated = ZonedDateTime.of(2000, 4, 1, 0, 0, 0, 0, ZoneId.systemDefault());
        long lastRevision = -1;

        for (KeyValueEntry kve : watcher.entries) {
            assertTrue(kve.getCreated().isAfter(lastCreated) || kve.getCreated().isEqual(lastCreated));
            lastCreated = kve.getCreated();

            assertTrue(lastRevision < kve.getRevision());
            lastRevision = kve.getRevision();

            Object expected = expectedKves[aix++];
            if (expected == null) {
                assertSame(KeyValueOperation.PUT, kve.getOperation());
                assertTrue(kve.getValue() == null || kve.getValue().length == 0);
                assertEquals(0, kve.getDataLen());
            }
            else if (expected instanceof String) {
                assertSame(KeyValueOperation.PUT, kve.getOperation());
                String s = (String) expected;
                if (watcher.metaOnly) {
                    assertTrue(kve.getValue() == null || kve.getValue().length == 0);
                    assertEquals(s.length(), kve.getDataLen());
                }
                else {
                    assertNotNull(kve.getValue());
                    assertEquals(s.length(), kve.getDataLen());
                    assertEquals(s, kve.getValueAsString());
                }
            }
            else {
                assertTrue(kve.getValue() == null || kve.getValue().length == 0);
                assertEquals(0, kve.getDataLen());
                assertSame(expected, kve.getOperation());
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Kv tester - Used to un-async calls since the tests are based on strict ordering of things
    // ----------------------------------------------------------------------------------------------------
    @SuppressWarnings({"SameParameterValue", "UnusedReturnValue"})
    static class KvTester {
        final KeyValueManagement kvm;
        final NatsClient natsClient;
        final String bucket;
        final KeyValueStatus status;
        final NatsVertxKeyValue kv;
        final AtomicReference<Throwable> executionError;

        public KvTester() throws IOException, JetStreamApiException, InterruptedException {
            this(null, null);
        }

        public KvTester(String bucketName) throws IOException, JetStreamApiException, InterruptedException {
            this(bucketName, b -> {});
        }

        public KvTester(java.util.function.Consumer<KeyValueConfiguration.Builder> customizer) throws IOException, JetStreamApiException, InterruptedException {
            this(unique(), customizer);
        }

        public KvTester(String bucket,
                        java.util.function.Consumer<KeyValueConfiguration.Builder> customizer)
            throws IOException, JetStreamApiException, InterruptedException {

            kvm = testRunner.nc.keyValueManagement();
            natsClient = getNatsClient();

            this.bucket = bucket;
            if (bucket== null) {
                status = null;
            }
            else {
                KeyValueConfiguration.Builder builder =
                    KeyValueConfiguration.builder(this.bucket)
                        .storageType(StorageType.Memory);
                customizer.accept(builder);
                status = kvm.create(builder.build());
            }

            executionError = new AtomicReference<>();
            kv = keyValue(natsClient, bucket);
        }

        public void assertThrew(Class<?> clazz) {
            assertNotNull(executionError.get());
            assertEquals(clazz, executionError.get().getClass());
        }

        private <T> T execute(Supplier<Future<T>> supplier) throws InterruptedException {
            executionError.set(null);
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<T> tRef = new AtomicReference<>();
            final Future<T> f = supplier.get();
            f.onSuccess(event -> {
                tRef.set(event);
                latch.countDown();
            }).onFailure(event -> {
                executionError.set(event);
                latch.countDown();
            });
            if (!latch.await(1, TimeUnit.SECONDS)) {
                executionError.set(new IOException("Execution Timeout"));
            }
            return tRef.get();
        }

        KeyValueEntry get(String key) throws InterruptedException {
            return execute(() -> kv.get(key));
        }

        KeyValueEntry get(String key, long revision) throws InterruptedException {
            return execute(() -> kv.get(key, revision));
        }

        Long put(String key, byte[] value) throws InterruptedException {
            return execute(() -> kv.put(key, value));
        }

        Long put(String key, String value) throws InterruptedException {
            return execute(() -> kv.put(key, value.getBytes(StandardCharsets.UTF_8)));
        }

        Long put(String key, Number value) throws InterruptedException {
            return execute(() -> kv.put(key, value.toString().getBytes(StandardCharsets.US_ASCII)));
        }

        Long create(String key, byte[] value) throws InterruptedException {
            return execute(() -> kv.create(key, value));
        }

        Long update(String key, byte[] value, long expectedRevision) throws InterruptedException {
            return execute(() -> kv.update(key, value, expectedRevision));
        }

        NatsKeyValueWatchSubscription watch(String key, KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws InterruptedException {
            return execute(() -> kv.watch(key, watcher, watchOptions));
        }

        NatsKeyValueWatchSubscription watch(String key, KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws InterruptedException {
            return execute(() -> kv.watch(key, watcher, fromRevision, watchOptions));
        }

        NatsKeyValueWatchSubscription watch(List<String> keys, KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws InterruptedException {
            return execute(() -> kv.watch(keys, watcher, watchOptions));
        }

        NatsKeyValueWatchSubscription watch(List<String> keys, KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws InterruptedException {
            return execute(() -> kv.watch(keys, watcher, fromRevision, watchOptions));
        }

        NatsKeyValueWatchSubscription watchAll(KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws InterruptedException {
            return execute(() -> kv.watchAll(watcher, watchOptions));
        }

        NatsKeyValueWatchSubscription watchAll(KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws InterruptedException {
            return execute(() -> kv.watchAll(watcher, fromRevision, watchOptions));
        }

        Long update(String key, String value, long expectedRevision) throws InterruptedException {
            return execute(() -> kv.update(key, value, expectedRevision));
        }

        void delete(String key) throws InterruptedException {
            execute(() -> kv.delete(key));
        }

        void delete(String key, long expectedRevision) throws InterruptedException {
            execute(() -> kv.delete(key, expectedRevision));
        }

        void purge(String key) throws InterruptedException {
            execute(() -> kv.purge(key));
        }

        void purge(String key, long expectedRevision) throws InterruptedException {
            execute(() -> kv.purge(key, expectedRevision));
        }

        NatsVertxKeyValue keyValue(NatsClient natsClient, String bucketName) throws InterruptedException {
            return execute(() -> natsClient.keyValue(bucketName));
        }

        List<String> keys() throws InterruptedException {
            return execute(kv::keys);
        }

        List<String> keys(String filter) throws InterruptedException {
            return execute(() -> kv.keys(filter));
        }

        List<String> keys(List<String> filters) throws InterruptedException {
            return execute(() -> kv.keys(filters));
        }

        List<KeyValueEntry> history(String key) throws InterruptedException {
            return execute(() -> kv.history(key));
        }

        void purgeDeletes() throws InterruptedException {
            execute(kv::purgeDeletes);
        }

        void purgeDeletes(KeyValuePurgeOptions options) throws InterruptedException {
            execute(() -> kv.purgeDeletes(options));
        }

        KeyValueStatus getStatus() throws InterruptedException {
            return execute(kv::getStatus);
        }
    }
}
