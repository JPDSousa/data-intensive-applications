package org.example.kv.lsm.sstable

import org.example.kv.LogKeyValueStore
import org.example.kv.LogKeyValueStoreFactory
import org.example.kv.lsm.OpenSegment
import org.example.kv.lsm.OpenSegmentFactory
import org.example.kv.lsm.Segment
import org.example.kv.lsm.SegmentDirectory
import org.example.log.LogFactory
import java.util.concurrent.atomic.AtomicInteger

private class SortedMapStringTable<K: Comparable<K>, V>(
    private val memTable: MemTable<K, V>,
    private val dump: LogKeyValueStore<K, V>,
    private val segmentThreshold: Long,
    private var dumped: Boolean = false
)
    : OpenSegment<K, V> {

    override fun put(key: K, value: V) {
        when(dumped) {
            true -> throw IllegalStateException("Cannot write to a SSTable which has already been dumped.")
            false -> memTable.put(key, value)
        }
    }

    override fun get(key: K, offset: Long?): V? = when(dumped) {
        true -> dump.get(key, offset)
        // offset is ignored here. Is this problematic?
        false -> memTable.get(key)
    }

    override fun delete(key: K) {
        when(dumped) {
            true -> throw IllegalStateException("Cannot delete entries to a SSTable which has already been dumped.")
            false -> memTable.delete(key)
        }
    }

    override fun clear() {
        when(dumped) {
            true -> throw IllegalStateException("Cannot clear a SSTable which has already been dumped.")
            false -> memTable.clear()
        }
    }

    private val size: Long
    get() = when (dumped) {
        true -> dump.size
        false -> memTable.byteSize
    }

    override fun closeSegment(): Segment<K, V> = when(dumped) {
        true -> throw IllegalStateException("Cannot dump a SSTable which has already been dumped.")
        false -> {
            // TODO we might want to prune tombstones to optimize storage, or leave this as is to optimize reads of
            //      deleted keys
            memTable.forEach(dump::append)
            dumped = true
            memTable.clear()
            Segment(dump, segmentThreshold)
        }
    }

    override fun getWithTombstone(key: K, offset: Long?): V? = get(key)

    override fun isFull(): Boolean = size >= segmentThreshold

}

class SSTableOpenSegmentFactory<K: Comparable<K>, V>(
    private val segmentDirectory: SegmentDirectory,
    private val memTableFactory: MemTableFactory<K, V>,
    private val logFactory: LogFactory<Map.Entry<K, V>>,
    private val keyValueStoreFactory: LogKeyValueStoreFactory<K, V>,
    private val segmentThreshold: Long,
    private val segCounter: AtomicInteger = AtomicInteger()
) : OpenSegmentFactory<K, V> {

    override fun createOpenSegment(): OpenSegment<K, V> {
        val segmentId = segCounter.getAndIncrement()

        return segmentDirectory.createSegmentFile(segmentId)
            .let { logFactory.create(it) }
            .let { keyValueStoreFactory.createFromPair(it) }
            .let { SortedMapStringTable(
                memTableFactory.createMemTable(segmentDirectory, segmentId),
                it,
                segmentThreshold
            ) }
    }
}