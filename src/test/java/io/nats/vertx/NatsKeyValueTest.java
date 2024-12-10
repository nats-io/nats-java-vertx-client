package io.nats.vertx;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.NatsKeyValueWatchSubscription;
import io.nats.client.support.NatsKeyValueUtil;
import io.vertx.core.Future;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.nats.client.api.KeyValueWatchOption.*;
import static io.nats.vertx.TestUtils2.*;
import static org.junit.jupiter.api.Assertions.*;

public class NatsKeyValueTest {
    public static final String META_KEY   = "meta-test-key";
    public static final String META_VALUE = "meta-test-value";

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

        // get the kv management context
        Map<String, String> metadata = new HashMap<>();
        metadata.put(META_KEY, META_VALUE);

        // create the bucket
        String desc = "desc" + unique();
        KvProxy proxy = new KvProxy(b ->
            b.description(desc).maxHistoryPerKey(3).metadata(metadata));

        assertInitialStatus(proxy.status, proxy.bucket, desc);

        // get the kv context for the specific bucket

        assertEquals(proxy.bucket, proxy.kv.getBucketName());
        KeyValueStatus status = proxy.getStatus();
        assertInitialStatus(status, proxy.bucket, desc);

        // Put some keys. Each key is put in a subject in the bucket (stream)
        // The put returns the sequence number in the bucket (stream)
        assertEquals(1, proxy.put(byteKey, byteValue1.getBytes()));
        assertEquals(2, proxy.put(stringKey, stringValue1));
        assertEquals(3, proxy.put(longKey, 1));

        // retrieve the values. all types are stored as bytes
        // so you can always get the bytes directly
        assertEquals(byteValue1, new String(proxy.get(byteKey).getValue()));
        assertEquals(stringValue1, new String(proxy.get(stringKey).getValue()));
        assertEquals("1", new String(proxy.get(longKey).getValue()));

        // if you know the value is not binary and can safely be read
        // as a UTF-8 string, the getStringValue method is ok to use
        assertEquals(byteValue1, proxy.get(byteKey).getValueAsString());
        assertEquals(stringValue1, proxy.get(stringKey).getValueAsString());
        assertEquals("1", proxy.get(longKey).getValueAsString());

        // if you know the value is a long, you can use
        // the getLongValue method
        // if it's not a number a NumberFormatException is thrown
        assertEquals(1, proxy.get(longKey).getValueAsLong());
        assertThrows(NumberFormatException.class, () -> proxy.get(stringKey).getValueAsLong());

        // going to manually track history for verification later
        List<Object> byteHistory = new ArrayList<>();
        List<Object> stringHistory = new ArrayList<>();
        List<Object> longHistory = new ArrayList<>();

        // entry gives detail about the latest entry of the key
        byteHistory.add(
            assertEntry(proxy.bucket, byteKey, KeyValueOperation.PUT, 1, byteValue1, now, proxy.get(byteKey)));

        stringHistory.add(
            assertEntry(proxy.bucket, stringKey, KeyValueOperation.PUT, 2, stringValue1, now, proxy.get(stringKey)));

        longHistory.add(
            assertEntry(proxy.bucket, longKey, KeyValueOperation.PUT, 3, "1", now, proxy.get(longKey)));

        // history gives detail about the key
        assertHistory(byteHistory, proxy.history(byteKey));
        assertHistory(stringHistory, proxy.history(stringKey));
        assertHistory(longHistory, proxy.history(longKey));

        // let's check the bucket info
        status = proxy.kvm.getStatus(proxy.bucket);
        assertState(status, 3, 3);
        status = proxy.kvm.getBucketInfo(proxy.bucket); // coverage for deprecated
        assertState(status, 3, 3);

        // delete a key. Its entry will still exist, but its value is null
        proxy.delete(byteKey);
        assertNull(proxy.get(byteKey));
        byteHistory.add(KeyValueOperation.DELETE);
        assertHistory(byteHistory, proxy.history(byteKey));

        // hashCode coverage
        assertEquals(byteHistory.get(0).hashCode(), byteHistory.get(0).hashCode());
        assertNotEquals(byteHistory.get(0).hashCode(), byteHistory.get(1).hashCode());

        // let's check the bucket info
        status = proxy.kvm.getStatus(proxy.bucket);
        assertState(status, 4, 4);

        // if the key has been deleted no etnry is returned
        assertNull(proxy.get(byteKey));

        // if the key does not exist (no history) there is no entry
        assertNull(proxy.get(notFoundKey));

        // Update values. You can even update a deleted key
        assertEquals(5, proxy.put(byteKey, byteValue2.getBytes()));
        assertEquals(6, proxy.put(stringKey, stringValue2));
        assertEquals(7, proxy.put(longKey, 2));

        // values after updates
        assertEquals(byteValue2, new String(proxy.get(byteKey).getValue()));
        assertEquals(stringValue2, proxy.get(stringKey).getValueAsString());
        assertEquals(2, proxy.get(longKey).getValueAsLong());

        // entry and history after update
        byteHistory.add(
            assertEntry(proxy.bucket, byteKey, KeyValueOperation.PUT, 5, byteValue2, now, proxy.get(byteKey)));
        assertHistory(byteHistory, proxy.history(byteKey));

        stringHistory.add(
            assertEntry(proxy.bucket, stringKey, KeyValueOperation.PUT, 6, stringValue2, now, proxy.get(stringKey)));
        assertHistory(stringHistory, proxy.history(stringKey));

        longHistory.add(
            assertEntry(proxy.bucket, longKey, KeyValueOperation.PUT, 7, "2", now, proxy.get(longKey)));
        assertHistory(longHistory, proxy.history(longKey));

        // let's check the bucket info
        status = proxy.kvm.getStatus(proxy.bucket);
        assertState(status, 7, 7);

        // make sure it only keeps the correct amount of history
        assertEquals(8, proxy.put(longKey, 3));
        assertEquals(3, proxy.get(longKey).getValueAsLong());

        longHistory.add(
            assertEntry(proxy.bucket, longKey, KeyValueOperation.PUT, 8, "3", now, proxy.get(longKey)));
        assertHistory(longHistory, proxy.history(longKey));

        status = proxy.kvm.getStatus(proxy.bucket);
        assertState(status, 8, 8);

        // this would be the 4th entry for the longKey
        // sp the total records will stay the same
        assertEquals(9, proxy.put(longKey, 4));
        assertEquals(4, proxy.get(longKey).getValueAsLong());

        // history only retains 3 records
        longHistory.remove(0);
        longHistory.add(
            assertEntry(proxy.bucket, longKey, KeyValueOperation.PUT, 9, "4", now, proxy.get(longKey)));
        assertHistory(longHistory, proxy.history(longKey));

        // record count does not increase
        status = proxy.kvm.getStatus(proxy.bucket);
        assertState(status, 8, 9);

        assertKeys(proxy.keys(), byteKey, stringKey, longKey);
        assertKeys(proxy.keys("key.>"), byteKey, stringKey, longKey);
        assertKeys(proxy.keys(byteKey), byteKey);
        assertKeys(proxy.keys(Arrays.asList(longKey, stringKey)), longKey, stringKey);

        // purge
        proxy.purge(longKey);
        longHistory.clear();
        assertNull(proxy.get(longKey));
        longHistory.add(KeyValueOperation.PURGE);
        assertHistory(longHistory, proxy.history(longKey));

        status = proxy.kvm.getStatus(proxy.bucket);
        assertState(status, 6, 10);

        // only 2 keys now
        assertKeys(proxy.keys(), byteKey, stringKey);

        proxy.purge(byteKey);
        byteHistory.clear();
        assertNull(proxy.get(byteKey));
        byteHistory.add(KeyValueOperation.PURGE);
        assertHistory(byteHistory, proxy.history(byteKey));

        status = proxy.kvm.getStatus(proxy.bucket);
        assertState(status, 4, 11);

        // only 1 key now
        assertKeys(proxy.keys(), stringKey);

        proxy.purge(stringKey);
        stringHistory.clear();
        assertNull(proxy.get(stringKey));
        stringHistory.add(KeyValueOperation.PURGE);
        assertHistory(stringHistory, proxy.history(stringKey));

        status = proxy.kvm.getStatus(proxy.bucket);
        assertState(status, 3, 12);

        // no more keys left
        assertKeys(proxy.keys());

        // clear things
        KeyValuePurgeOptions kvpo = KeyValuePurgeOptions.builder().deleteMarkersNoThreshold().build();
        proxy.purgeDeletes(kvpo);
        status = proxy.kvm.getStatus(proxy.bucket);
        assertState(status, 0, 12);

        longHistory.clear();
        assertHistory(longHistory, proxy.history(longKey));

        stringHistory.clear();
        assertHistory(stringHistory, proxy.history(stringKey));

        // put some more
        assertEquals(13, proxy.put(longKey, 110));
        longHistory.add(
            assertEntry(proxy.bucket, longKey, KeyValueOperation.PUT, 13, "110", now, proxy.get(longKey)));

        assertEquals(14, proxy.put(longKey, 111));
        longHistory.add(
            assertEntry(proxy.bucket, longKey, KeyValueOperation.PUT, 14, "111", now, proxy.get(longKey)));

        assertEquals(15, proxy.put(longKey, 112));
        longHistory.add(
            assertEntry(proxy.bucket, longKey, KeyValueOperation.PUT, 15, "112", now, proxy.get(longKey)));

        assertEquals(16, proxy.put(stringKey, stringValue1));
        stringHistory.add(
            assertEntry(proxy.bucket, stringKey, KeyValueOperation.PUT, 16, stringValue1, now, proxy.get(stringKey)));

        assertEquals(17, proxy.put(stringKey, stringValue2));
        stringHistory.add(
            assertEntry(proxy.bucket, stringKey, KeyValueOperation.PUT, 17, stringValue2, now, proxy.get(stringKey)));

        assertHistory(longHistory, proxy.history(longKey));
        assertHistory(stringHistory, proxy.history(stringKey));

        status = proxy.kvm.getStatus(proxy.bucket);
        assertState(status, 5, 17);

        // delete the bucket
        proxy.kvm.delete(proxy.bucket);
        assertThrows(JetStreamApiException.class, () -> proxy.kvm.delete(proxy.bucket));
        assertThrows(JetStreamApiException.class, () -> proxy.kvm.getStatus(proxy.bucket));

        assertEquals(0, proxy.kvm.getBucketNames().size());
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
        assertEquals(-1, status.getMaxValueSize()); // COVERAGE for deprecated
        assertEquals(-1, kvc.getMaxValueSize());
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
        KvProxy proxy = new KvProxy(b -> b.maxHistoryPerKey(2));

        String key = unique();

        long seq1 = proxy.put(key, 1);
        long seq2 = proxy.put(key, 2);
        long seq3 = proxy.put(key, 3);

        KeyValueEntry kve = proxy.get(key);
        assertNotNull(kve);
        assertEquals(3, kve.getValueAsLong());

        kve = proxy.get(key, seq3);
        assertNotNull(kve);
        assertEquals(3, kve.getValueAsLong());

        kve = proxy.get(key, seq2);
        assertNotNull(kve);
        assertEquals(2, kve.getValueAsLong());

        kve = proxy.get(key, seq1);
        assertNull(kve);

        kve = proxy.get("notkey", seq3);
        assertNull(kve);
    }

    @Test
    public void testKeys() throws Exception {
        KvProxy proxy = new KvProxy();

        for (int x = 1; x <= 10; x++) {
            proxy.put("k" + x, x);
        }

        List<String> keys = proxy.keys();
        assertEquals(10, keys.size());
        for (int x = 1; x <= 10; x++) {
            assertTrue(keys.contains("k" + x));
        }
    }

    @Test
    public void testMaxHistoryPerKey() throws Exception {
        KvProxy proxy1 = new KvProxy();

        String key = unique();
        proxy1.put(key, 1);
        proxy1.put(key, 2);

        List<KeyValueEntry> history = proxy1.history(key);
        assertEquals(1, history.size());
        assertEquals(2, history.get(0).getValueAsLong());

        KvProxy proxy2 = new KvProxy(b -> b.maxHistoryPerKey(2));

        key = unique();
        proxy2.put(key, 1);
        proxy2.put(key, 2);
        proxy2.put(key, 3);

        history = proxy2.history(key);
        assertEquals(2, history.size());
        assertEquals(2, history.get(0).getValueAsLong());
        assertEquals(3, history.get(1).getValueAsLong());
    }

    @Test
    public void testCreateUpdate() throws Exception {
        KvProxy proxy = new KvProxy();

        assertEquals(proxy.bucket, proxy.status.getBucketName());
        assertNull(proxy.status.getDescription());
        assertEquals(1, proxy.status.getMaxHistoryPerKey());
        assertEquals(-1, proxy.status.getMaxBucketSize());
        assertEquals(-1, proxy.status.getMaximumValueSize());
        assertEquals(Duration.ZERO, proxy.status.getTtl());
        assertEquals(StorageType.Memory, proxy.status.getStorageType());
        assertEquals(1, proxy.status.getReplicas());
        assertEquals(0, proxy.status.getEntryCount());
        assertEquals("JetStream", proxy.status.getBackingStore());

        String key = unique();
        proxy.put(key, 1);
        proxy.put(key, 2);

        List<KeyValueEntry> history = proxy.history(key);
        assertEquals(1, history.size());
        assertEquals(2, history.get(0).getValueAsLong());

        boolean compression = true;
        String desc = unique();
        KeyValueConfiguration kvc = KeyValueConfiguration.builder(proxy.status.getConfiguration())
            .description(desc)
            .maxHistoryPerKey(3)
            .maxBucketSize(10_000)
            .maximumValueSize(100)
            .ttl(Duration.ofHours(1))
            .compression(compression)
            .build();

        KeyValueStatus kvs = proxy.kvm.update(kvc);

        assertEquals(proxy.bucket, kvs.getBucketName());
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

        history = proxy.history(key);
        assertEquals(1, history.size());
        assertEquals(2, history.get(0).getValueAsLong());

        KeyValueConfiguration kvcStor = KeyValueConfiguration.builder(kvs.getConfiguration())
            .storageType(StorageType.File)
            .build();
        assertThrows(JetStreamApiException.class, () -> proxy.kvm.update(kvcStor));
    }

    @Test
    public void testHistoryDeletePurge() throws Exception {
        KvProxy proxy = new KvProxy(b -> b.maxHistoryPerKey(64));

        String key = unique();
        proxy.put(key, "a");
        proxy.put(key, "b");
        proxy.put(key, "c");
        List<KeyValueEntry> list = proxy.history(key);
        assertEquals(3, list.size());

        proxy.delete(key);
        list = proxy.history(key);
        assertEquals(4, list.size());

        proxy.purge(key);
        list = proxy.history(key);
        assertEquals(1, list.size());
    }

    @Test
    public void testAtomicDeleteAtomicPurge() throws Exception {
        KvProxy proxy = new KvProxy(b -> b.maxHistoryPerKey(64));

        String key = unique();
        proxy.put(key, "a");
        proxy.put(key, "b");
        proxy.put(key, "c");
        assertEquals(3, proxy.get(key).getRevision());

        // Delete wrong revision rejected
        proxy.delete(key, 1);
        proxy.assertThrew(JetStreamApiException.class);

        // Correct revision writes tombstone and bumps revision
        proxy.delete(key, 3);

        assertHistory(Arrays.asList(
                proxy.get(key, 1L),
                proxy.get(key, 2L),
                proxy.get(key, 3L),
                KeyValueOperation.DELETE),
            proxy.history(key));

        // Wrong revision rejected again
        proxy.delete(key, 3);
        proxy.assertThrew(JetStreamApiException.class);

        // Delete is idempotent: two consecutive tombstones
        proxy.delete(key, 4);

        assertHistory(Arrays.asList(
                proxy.get(key, 1L),
                proxy.get(key, 2L),
                proxy.get(key, 3L),
                KeyValueOperation.DELETE,
                KeyValueOperation.DELETE),
            proxy.history(key));

        // Purge wrong revision rejected
        proxy.purge(key, 1);
        proxy.assertThrew(JetStreamApiException.class);

        // Correct revision writes roll-up purge tombstone
        proxy.purge(key, 5);

        assertHistory(Arrays.asList(KeyValueOperation.PURGE), proxy.history(key));
    }

    @Test
    public void testPurgeDeletes() throws Exception {
        KvProxy proxy = new KvProxy(b -> b.maxHistoryPerKey(64));

        String key1 = unique();
        String key2 = unique();
        String key3 = unique();
        String key4 = unique();
        proxy.put(key1, "a");
        proxy.delete(key1);
        proxy.put(key2, "b");
        proxy.put(key3, "c");
        proxy.put(key4, "d");
        proxy.purge(key4);

        JetStream js = testRunner.nc.jetStream();

        assertPurgeDeleteEntries(js, proxy.bucket, new String[]{"a", null, "b", "c", null});

        // default purge deletes uses the default threshold
        // so no markers will be deleted
        proxy.purgeDeletes();
        assertPurgeDeleteEntries(js, proxy.bucket, new String[]{null, "b", "c", null});

        // deleteMarkersThreshold of 0 the default threshold
        // so no markers will be deleted
        proxy.purgeDeletes(KeyValuePurgeOptions.builder().deleteMarkersThreshold(0).build());
        assertPurgeDeleteEntries(js, proxy.bucket, new String[]{null, "b", "c", null});

        // no threshold causes all to be removed
        proxy.purgeDeletes(KeyValuePurgeOptions.builder().deleteMarkersNoThreshold().build());
        assertPurgeDeleteEntries(js, proxy.bucket, new String[]{"b", "c"});
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
        KvProxy proxy = new KvProxy(b -> b.maxHistoryPerKey(64));

        String key = unique();
        // 1. allowed to create something that does not exist
        long rev1 = proxy.create(key, "a".getBytes());

        // 2. allowed to update with proper revision
        proxy.update(key, "ab".getBytes(), rev1);

        // 3. not allowed to update with wrong revision
        proxy.update(key, "zzz".getBytes(), rev1);
        proxy.assertThrew(JetStreamApiException.class);

        // 4. not allowed to create a key that exists
        proxy.create(key, "zzz".getBytes());
        proxy.assertThrew(JetStreamApiException.class);

        // 5. not allowed to update a key that does not exist
        proxy.update(key, "zzz".getBytes(), 1);
        proxy.assertThrew(JetStreamApiException.class);

        // 6. allowed to create a key that is deleted
        proxy.delete(key);
        proxy.create(key, "abc".getBytes());

        // 7. allowed to update a key that is deleted, as long as you have it's revision
        proxy.delete(key);
        testRunner.nc.flush(Duration.ofSeconds(1));

        sleep(200); // a little pause to make sure things get flushed
        List<KeyValueEntry> hist = proxy.history(key);
        proxy.update(key, "abcd".getBytes(), hist.get(hist.size() - 1).getRevision());

        // 8. allowed to create a key that is purged
        proxy.purge(key);
        proxy.create(key, "abcde".getBytes());

        // 9. allowed to update a key that is deleted, as long as you have its revision
        proxy.purge(key);

        sleep(200); // a little pause to make sure things get flushed
        hist = proxy.history(key);
        proxy.update(key, "abcdef".getBytes(), hist.get(hist.size() - 1).getRevision());
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

    interface TestWatchSubSupplier {
        NatsKeyValueWatchSubscription get(KeyValue kv) throws Exception;
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
        _testWatch(key1FromRevisionAfterWatcher, key1FromRevisionExpecteds, 2, kv -> kv.watch(TEST_WATCH_KEY_1, key1FromRevisionAfterWatcher, 2, key1FromRevisionAfterWatcher.watchOptions));
        _testWatch(allFromRevisionAfterWatcher, allFromRevisionExpecteds, 2, kv -> kv.watchAll(allFromRevisionAfterWatcher, 2, allFromRevisionAfterWatcher.watchOptions));

        _testWatch(multipleFullWatcher, allExpecteds, -1, kv -> kv.watch(allKeys, multipleFullWatcher, multipleFullWatcher.watchOptions));
        _testWatch(multipleMetaWatcher, allExpecteds, -1, kv -> kv.watch(allKeys, multipleMetaWatcher, multipleMetaWatcher.watchOptions));
    }

    private void _testWatch(TestKeyValueWatcher watcher, Object[] expectedKves, long fromRevision, TestWatchSubSupplier supplier) throws Exception {
        KeyValueManagement kvm = testRunner.nc.keyValueManagement();

        String bucket = unique() + watcher.name + "Bucket";
        kvm.create(KeyValueConfiguration.builder()
            .name(bucket)
            .maxHistoryPerKey(10)
            .storageType(StorageType.Memory)
            .build());

        KeyValue kv = testRunner.nc.keyValue(bucket);

        NatsKeyValueWatchSubscription sub = null;

        if (watcher.beforeWatcher) {
            sub = supplier.get(kv);
        }

        if (fromRevision == -1) {
            kv.put(TEST_WATCH_KEY_1, "a");
            kv.put(TEST_WATCH_KEY_1, "aa");
            kv.put(TEST_WATCH_KEY_2, "z");
            kv.put(TEST_WATCH_KEY_2, "zz");
            kv.delete(TEST_WATCH_KEY_1);
            kv.delete(TEST_WATCH_KEY_2);
            kv.put(TEST_WATCH_KEY_1, "aaa");
            kv.put(TEST_WATCH_KEY_2, "zzz");
            kv.delete(TEST_WATCH_KEY_1);
            kv.purge(TEST_WATCH_KEY_1);
            kv.put(TEST_WATCH_KEY_NULL, (byte[]) null);
        }
        else {
            kv.put(TEST_WATCH_KEY_1, "a");
            kv.put(TEST_WATCH_KEY_1, "aa");
            kv.put(TEST_WATCH_KEY_2, "z");
            kv.put(TEST_WATCH_KEY_2, "zz");
            kv.delete(TEST_WATCH_KEY_1);
            kv.delete(TEST_WATCH_KEY_2);
            kv.put(TEST_WATCH_KEY_1, "aaa");
            kv.put(TEST_WATCH_KEY_2, "zzz");
        }

        if (!watcher.beforeWatcher) {
            sub = supplier.get(kv);
        }

        sleep(1500); // give time for the watches to get messages

        validateWatcher(expectedKves, watcher);
        //noinspection ConstantConditions
        sub.unsubscribe();
        kvm.delete(bucket);
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

    @Test
    public void testMirrorSourceBuilderPrefixConversion() throws Exception {
        String bucket = unique();
        String name = unique();
        String kvName = "KV_" + name;
        KeyValueConfiguration kvc = KeyValueConfiguration.builder()
            .name(bucket)
            .mirror(Mirror.builder().name(name).build())
            .build();
        assertEquals(kvName, kvc.getBackingConfig().getMirror().getName());

        kvc = KeyValueConfiguration.builder()
            .name(bucket)
            .mirror(Mirror.builder().name(kvName).build())
            .build();
        assertEquals(kvName, kvc.getBackingConfig().getMirror().getName());

        Source s1 = Source.builder().name("s1").build();
        Source s2 = Source.builder().name("s2").build();
        Source s3 = Source.builder().name("s3").build();
        Source s4 = Source.builder().name("s4").build();
        Source s5 = Source.builder().name("KV_s5").build();
        Source s6 = Source.builder().name("KV_s6").build();

        kvc = KeyValueConfiguration.builder()
            .name(bucket)
            .sources(s3, s4)
            .sources(Arrays.asList(s1, s2))
            .addSources(s1, s2)
            .addSources(Arrays.asList(s1, s2, null))
            .addSources(s3, s4)
            .addSource(null)
            .addSource(s5)
            .addSource(s5)
            .addSources(s6)
            .addSources((Source[])null)
            .addSources((Collection<Source>)null)
            .build();

        assertEquals(6, kvc.getBackingConfig().getSources().size());
        List<String> names = new ArrayList<>();
        for (Source source : kvc.getBackingConfig().getSources()) {
            names.add(source.getName());
        }
        assertTrue(names.contains("KV_s1"));
        assertTrue(names.contains("KV_s2"));
        assertTrue(names.contains("KV_s3"));
        assertTrue(names.contains("KV_s4"));
        assertTrue(names.contains("KV_s5"));
        assertTrue(names.contains("KV_s6"));
    }

    // ----------------------------------------------------------------------------------------------------
    // UTILITIES
    // ----------------------------------------------------------------------------------------------------
    static TestsRunner testRunner;
    static int port;

    @AfterAll
    public static void afterAll() throws Exception {
        testRunner.close();
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        testRunner = TestUtils2.startServer();
        port = testRunner.natsServerRunner.getPort();
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        cleanupJs();
    }

    @AfterEach
    public void afterEach() throws Exception {
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

    static class KvProxy {
        final KeyValueManagement kvm;
        final String bucket;
        final KeyValueStatus status;
        final NatsClient natsClient;
        final NatsKeyValue kv;
        final AtomicReference<Throwable> executionError = new AtomicReference<>();

        public KvProxy() throws IOException, JetStreamApiException, InterruptedException {
            this(unique(), b -> {});
        }

        public KvProxy(String bucketName) throws IOException, JetStreamApiException, InterruptedException {
            this(bucketName, b -> {});
        }

        public KvProxy(java.util.function.Consumer<KeyValueConfiguration.Builder> customizer) throws IOException, JetStreamApiException, InterruptedException {
            this(unique(), customizer);
        }

        public KvProxy(String bucket,
                       java.util.function.Consumer<KeyValueConfiguration.Builder> customizer)
            throws IOException, JetStreamApiException, InterruptedException
        {
            this.bucket = bucket;
            KeyValueConfiguration.Builder builder =
                KeyValueConfiguration.builder(this.bucket)
                    .storageType(StorageType.Memory);

            customizer.accept(builder);

            kvm = testRunner.nc.keyValueManagement();
            status = kvm.create(builder.build());

            natsClient = getNatsClient();
            kv = keyValue(natsClient, this.bucket);
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
            throw new RuntimeException("Not Implemented");
        }


        NatsKeyValueWatchSubscription watch(String key, KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws InterruptedException {
            throw new RuntimeException("Not Implemented");
        }

        NatsKeyValueWatchSubscription watch(List<String> keys, KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws InterruptedException {
            throw new RuntimeException("Not Implemented");
        }

        NatsKeyValueWatchSubscription watch(List<String> keys, KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws InterruptedException {
            throw new RuntimeException("Not Implemented");
        }

        NatsKeyValueWatchSubscription watchAll(KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws InterruptedException {
            throw new RuntimeException("Not Implemented");
        }

        NatsKeyValueWatchSubscription watchAll(KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws InterruptedException {
            throw new RuntimeException("Not Implemented");
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

        NatsKeyValue keyValue(NatsClient natsClient, String bucketName) throws InterruptedException {
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
