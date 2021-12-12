package org.example.kv.lsm.sstable

import org.example.concepts.SerializationMixin
import org.example.kv.LogKeyValueStore
import org.example.kv.LogKeyValueStoreFactory
import org.example.kv.lsm.OpenSegment
import org.example.kv.lsm.OpenSegmentFactory
import org.example.kv.lsm.Segment
import org.example.kv.lsm.SegmentDirectory
import org.example.log.EntryLogFactory
import java.util.concurrent.atomic.AtomicInteger

private class SortedMapStringTable<K: Comparable<K>, V>(
    private val memTable: MemTable<K, V>,
    private val dump: LogKeyValueStore<K, V>,
    private val segmentThreshold: Long,
    private var dumped: Boolean = false
) : OpenSegment<K, V>, SerializationMixin {

    override fun put(key: K, value: V) {
        when(dumped) {
            true -> throw notOnDump("write")
            false -> memTable[key] = value
        }
    }

    override fun get(key: K, offset: Long?): V? = when(dumped) {
        true -> dump[key, offset]
        // offset is ignored here. Is this problematic?
        false -> memTable[key]
    }

    override fun contains(key: K): Boolean = when(dumped) {
        true -> key in dump
        false -> key in memTable
    }

    override fun delete(key: K) {
        when(dumped) {
            true -> throw notOnDump("delete entries on")
            false -> memTable.delete(key)
        }
    }

    override fun clear() {
        when(dumped) {
            true -> throw notOnDump("clear")
            false -> memTable.clear()
        }
    }

    override val byteLength: Long
    get() = when (dumped) {
        true -> dump.byteLength
        false -> memTable.byteLength
    }

    override fun closeSegment(): Segment<K, V> = when(dumped) {
        true -> throw notOnDump("dump")
        false -> {
            // TODO we might want to prune tombstones to optimize storage, or leave this as is to optimize reads of
            //      deleted keys
            memTable.forEach(dump::append)
            dumped = true
            memTable.clear()
            Segment(dump, segmentThreshold)
        }
    }

    override fun getWithTombstone(key: K, offset: Long?): V? = this[key]

    override fun isFull(): Boolean = byteLength >= segmentThreshold

    override val size: Int
        get() = when(dumped) {
            true -> dump.size
            false -> memTable.size
        }

    private fun notOnDump(operationName: String)
            = IllegalStateException("Cannot $operationName an SSTable which has already been dumped")

}

class SSTableOpenSegmentFactory<K: Comparable<K>, V>(
    private val segmentDirectory: SegmentDirectory,
    private val memTableFactory: MemTableFactory<K, V>,
    private val logFactory: EntryLogFactory<K, V>,
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