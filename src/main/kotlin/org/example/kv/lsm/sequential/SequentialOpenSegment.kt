package org.example.kv.lsm.sequential

import org.example.kv.LogKeyValueStore
import org.example.kv.LogKeyValueStoreFactory
import org.example.kv.TombstoneKeyValueStore
import org.example.kv.lsm.OpenSegment
import org.example.kv.lsm.OpenSegmentFactory
import org.example.kv.lsm.Segment
import org.example.kv.lsm.SegmentDirectory
import org.example.log.LogFactory
import java.util.concurrent.atomic.AtomicInteger

private class SequentialOpenSegment<K, V>(
    private val keyValueStore: LogKeyValueStore<K, V>,
    private val segmentThreshold: Long = 1024L * 1024L
): OpenSegment<K, V>, TombstoneKeyValueStore<K, V> by keyValueStore {

    override fun closeSegment(): Segment<K, V> = Segment(keyValueStore, segmentThreshold)

    override fun isFull(): Boolean = keyValueStore.size >= segmentThreshold
}

class SequentialOpenSegmentFactory<K, V>(
    private val segmentDirectory: SegmentDirectory,
    private val logFactory: LogFactory<Map.Entry<K, V>>,
    private val keyValueStoreFactory: LogKeyValueStoreFactory<K, V>,
    private val segCounter: AtomicInteger = AtomicInteger()
): OpenSegmentFactory<K, V> {

    override fun createOpenSegment(): OpenSegment<K, V> = segmentDirectory
        .createSegmentFile(segCounter.getAndIncrement())
        .let { logFactory.create(it) }
        .let { keyValueStoreFactory.createFromPair(it) }
        .let { SequentialOpenSegment(it) }

}
