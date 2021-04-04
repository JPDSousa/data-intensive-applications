package org.example.kv.lsm.sstable

import org.example.kv.LogBasedKeyValueStore
import org.example.kv.LogBasedKeyValueStoreFactory
import org.example.log.LogFactory
import org.example.kv.lsm.OpenSegment
import org.example.kv.lsm.Segment
import org.example.kv.lsm.SegmentDirectory
import org.example.kv.lsm.SegmentManager
import org.example.size.SizeCalculator
import java.util.*

private class SortedMapStringTable<K: Comparable<K>, V>(private val memTable: SortedMap<K, V>,
                                                        private val dump: LogBasedKeyValueStore<K, V>,
                                                        private val keySize: SizeCalculator<K>,
                                                        private val valueSize: SizeCalculator<V>,
                                                        private val tombstone: V,
                                                        private val segmentThreshold: Long,
                                                        private var dumped: Boolean = false)
    : OpenSegment<K, V> {

    private var memCacheSize: Long = 0L

    override fun put(key: K, value: V) {
        when(dumped) {
            true -> throw IllegalStateException("Cannot write to a SSTable which has already been dumped.")
            false -> {
                memTable[key] = value
                memCacheSize += keySize.sizeOf(key)
                memCacheSize += valueSize.sizeOf(value)
            }
        }
    }

    override fun get(key: K): V? = when(dumped) {
        true -> dump.get(key)
        false -> memTable[key]
    }

    override fun delete(key: K) {
        when(dumped) {
            true -> throw IllegalStateException("Cannot delete entries to a SSTable which has already been dumped.")
            false -> memTable[key] = tombstone
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
        false -> memCacheSize
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

    override fun getWithTombstone(key: K): V? = get(key)

    override fun isFull(): Boolean = size >= segmentThreshold

}

class SSTableSegmentManager<K: Comparable<K>, V>(private val segmentDirectory: SegmentDirectory,
                                                 private val logFactory: LogFactory<Map.Entry<K, V>>,
                                                 private val keyValueStoreFactory: LogBasedKeyValueStoreFactory<K, V>,
                                                 mergeStrategy: SSTableMergeStrategy<K, V>,
                                                 private val segmentThreshold: Long,
                                                 private val keySize: SizeCalculator<K>,
                                                 private val valueSize: SizeCalculator<V>,
                                                 private val tombstone: V)
    : SegmentManager<K, V>(segmentDirectory, logFactory, keyValueStoreFactory, mergeStrategy, segmentThreshold) {

    override fun createOpenSegment(): OpenSegment<K, V> = segmentDirectory
        .createSegmentFile()
        .let { logFactory.create(it) }
        .let { keyValueStoreFactory.createFromPair(it) }
        .let { SortedMapStringTable(TreeMap(), it, keySize, valueSize, tombstone, segmentThreshold) }
}
