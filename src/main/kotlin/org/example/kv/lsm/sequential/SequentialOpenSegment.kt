package org.example.kv.lsm.sequential

import org.example.kv.LogKeyValueStore
import org.example.kv.LogKeyValueStoreFactory
import org.example.kv.TombstoneKeyValueStore
import org.example.kv.lsm.*
import org.example.log.LogFactory

private class SequentialOpenSegment<K, V>(private val keyValueStore: LogKeyValueStore<K, V>,
                                          private val segmentThreshold: Long = 1024 * 1024):
    OpenSegment<K, V>, TombstoneKeyValueStore<K, V> by keyValueStore {

    override fun closeSegment(): Segment<K, V> = Segment(keyValueStore, segmentThreshold)

    override fun isFull(): Boolean = keyValueStore.size >= segmentThreshold
}

class SequentialSegmentManager<K, V>(private val segmentDirectory: SegmentDirectory,
                                     private val logFactory: LogFactory<Map.Entry<K, V>>,
                                     private val keyValueStoreFactory: LogKeyValueStoreFactory<K, V>,
                                     segmentThreshold: Long,
                                     mergeStrategy: SequentialLogMergeStrategy<K, V>
):
    SegmentManager<K, V>(segmentDirectory, logFactory, keyValueStoreFactory, mergeStrategy, segmentThreshold) {

    override fun createOpenSegment(): OpenSegment<K, V> = segmentDirectory
        .createSegmentFile()
        .let { logFactory.create(it) }
        .let { keyValueStoreFactory.createFromPair(it) }
        .let { SequentialOpenSegment(it) }
}
