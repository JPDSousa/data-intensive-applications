package org.example.kv.lsm

import mu.KotlinLogging
import org.example.kv.LogBasedKeyValueStore
import org.example.lsm.Segment
import org.example.lsm.SegmentFactory
import org.example.lsm.SegmentMergeStrategy
import java.util.*

internal class KeyValueLogMergeStrategy<K, V>(
        private val segmentFactory: SegmentFactory<LogBasedKeyValueStore<K, V>>):
        SegmentMergeStrategy<LogBasedKeyValueStore<K, V>> {

    private val logger = KotlinLogging.logger {}

    override fun merge(segmentsToCompact: List<Segment<LogBasedKeyValueStore<K, V>>>):
            List<Segment<LogBasedKeyValueStore<K, V>>> {

        logger.debug { "Triggering a compact operation" }
        val compactedSegments = LinkedList<Segment<LogBasedKeyValueStore<K, V>>>()

        var segCounter = 1
        // TODO compact according to segment size instead.
        val memorySegment = CompactedLSMSegment(segmentFactory, 1000)
        for (segment in segmentsToCompact) {

            logger.trace { "Compacting segment ${segCounter++}/${compactedSegments.size}" }
            segment.structure.log.useEntries { entries ->
                entries.forEach { entry ->
                    memorySegment.upsert(entry)
                    if (memorySegment.isFull()) {
                        logger.trace { "Current in-memory segment is full. Flushing to disk." }
                        compactedSegments.add(memorySegment.flush())
                    }
                }
            }
        }

        return compactedSegments
    }

}

private class CompactedLSMSegment<K, V>(private val factory: SegmentFactory<LogBasedKeyValueStore<K, V>>,
                                        private val limit: Int) {

    private val compactedContent = mutableMapOf<K, V>()

    fun upsert(entry: Pair<K, V>) = compactedContent.put(entry.first, entry.second)

    fun flush(): Segment<LogBasedKeyValueStore<K, V>> {
        val compacted = factory.createSegment()

        compacted.structure.putAll(compactedContent)
        compactedContent.clear()

        return compacted
    }

    fun isFull() = compactedContent.size >= limit

    fun isNotEmpty() = compactedContent.isNotEmpty()

}
