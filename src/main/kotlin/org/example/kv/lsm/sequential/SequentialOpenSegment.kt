package org.example.kv.lsm

import org.example.kv.LogBasedKeyValueStore
import org.example.kv.LogBasedKeyValueStoreFactory
import org.example.log.LogFactory

private class SequentialOpenSegment<K, V>(private val keyValueStore: LogBasedKeyValueStore<K, V>,
                                          private val segmentThreshold: Long = 1024 * 1024):
    OpenSegment<K, V> {

    override fun put(key: K, value: V) = keyValueStore.put(key, value)

    override fun getWithTombstone(key: K): V? = keyValueStore.getWithTombstone(key)

    override fun get(key: K): V? = keyValueStore.get(key)

    override fun delete(key: K) = keyValueStore.delete(key)

    override fun clear() = keyValueStore.clear()

    override fun closeSegment(): Segment<K, V> = Segment(keyValueStore, segmentThreshold)

    override fun isFull(): Boolean = keyValueStore.size >= segmentThreshold
}

class SequentialSegmentManager<K, V>(private val segmentDirectory: SegmentDirectory,
                                     private val logFactory: LogFactory<Map.Entry<K, V>>,
                                     private val keyValueStoreFactory: LogBasedKeyValueStoreFactory<K, V>,
                                     segmentThreshold: Long,
                                     mergeStrategy: SequentialLogMergeStrategy<K, V>):
    SegmentManager<K, V>(segmentDirectory, logFactory, keyValueStoreFactory, mergeStrategy, segmentThreshold) {

    override fun createOpenSegment(): OpenSegment<K, V> = segmentDirectory
        .createSegmentFile()
        .let { logFactory.create(it) }
        .let { keyValueStoreFactory.createFromPair(it) }
        .let { SequentialOpenSegment(it) }
}
