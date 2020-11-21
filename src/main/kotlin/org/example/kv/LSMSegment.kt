package org.example.kv

import org.example.log.Index
import org.example.log.Log
import org.example.lsm.Segment
import org.example.lsm.SegmentFactory
import java.util.concurrent.atomic.AtomicInteger

internal class LSMSegment<E, K, V>(override val structure: IndexedKeyValueStore<E, K, V>,
                                   private val segmentThreshold: Long) : Segment<IndexedKeyValueStore<E, K, V>> {

    override fun isClosed(): Boolean = structure.isClosed()

    override fun clear() = structure.clear()

    override val size: Long
        get() = structure.log.size()

    private fun IndexedKeyValueStore<E, K, V>.isClosed() = log.size() >= segmentThreshold
}

internal class LSMSegmentFactory<E, K, V>(private val resourceFactory: SegmentResourcesFactory<E, K, V>,
                                          private val segmentThreshold: Long,
                                          private var segmentCounter: AtomicInteger = AtomicInteger(0)):
        SegmentFactory<IndexedKeyValueStore<E, K, V>> {

    override fun createSegment(): Segment<IndexedKeyValueStore<E, K, V>> = LSMSegment(
            resourceFactory.createKeyValueStore(segmentCounter.getAndIncrement().toString()),
            segmentThreshold
    )
}

internal interface SegmentResourcesFactory<E, K, V> {

    fun createLog(segmentId: String): Log<E>

    fun createIndex(segmentId: String): Index<K>

    fun createEncoder(): EntryEncoder<E, K, V>

    fun tombstone(): V

    fun createKeyValueStore(segmentId: String) = IndexedKeyValueStore(
            createIndex(segmentId),
            createLog(segmentId),
            createEncoder(),
            tombstone()
    )

}
