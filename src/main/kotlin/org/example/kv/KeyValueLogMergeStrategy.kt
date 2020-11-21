package org.example.kv

import mu.KotlinLogging
import org.example.lsm.Segment
import org.example.lsm.SegmentFactory
import org.example.lsm.SegmentMergeStrategy
import java.util.*

internal class KeyValueLogMergeStrategy<E, K, V>(
        private val segmentFactory: SegmentFactory<IndexedKeyValueStore<E, K, V>>):
        SegmentMergeStrategy<IndexedKeyValueStore<E, K, V>> {

    private val logger = KotlinLogging.logger {}

    override fun merge(segmentsToCompact: List<Segment<IndexedKeyValueStore<E, K, V>>>):
            List<Segment<IndexedKeyValueStore<E, K, V>>> {

        logger.debug { "Triggering a compact operation" }
        val compactedSegments = LinkedList<Segment<IndexedKeyValueStore<E, K, V>>>()

        for (segment in segmentsToCompact) {
            // TODO compact according to segment size instead.
            val memorySegment = CompactedLSMSegment(segmentFactory, 1000)

            segment.structure.forEachEntry { entry ->
                memorySegment.upsert(entry)
                if (memorySegment.isFull()) {
                    compactedSegments.add(memorySegment.flush())
                }
            }

            if (memorySegment.isNotEmpty()) {
                compactedSegments.add(memorySegment.flush())
            }
        }

        return compactedSegments
    }

}

private class CompactedLSMSegment<E, K, V>(private val factory: SegmentFactory<IndexedKeyValueStore<E, K, V>>,
                                           private val limit: Int) {

    private val compactedContent = mutableMapOf<K, V>()

    fun upsert(entry: Pair<K, V>) = compactedContent.put(entry.first, entry.second)

    fun flush(): Segment<IndexedKeyValueStore<E, K, V>> {
        val compacted = factory.createSegment()

        compacted.structure.putAll(compactedContent)
        compactedContent.clear()

        return compacted
    }

    fun isFull() = compactedContent.size >= limit

    fun isNotEmpty() = compactedContent.isNotEmpty()

}
