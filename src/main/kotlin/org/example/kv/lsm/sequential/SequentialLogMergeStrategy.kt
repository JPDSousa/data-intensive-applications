package org.example.kv.lsm

import mu.KotlinLogging
import org.example.size.SizeCalculator
import java.util.*

class SequentialLogMergeStrategy<K, V>(private val segmentFactory: SegmentFactory<K, V>,
                                       private val segmentThreshold: Long,
                                       private val keySize: SizeCalculator<K>,
                                       private val valueSize: SizeCalculator<V>): SegmentMergeStrategy<K, V> {

    private val logger = KotlinLogging.logger {}

    override fun merge(segmentsToCompact: List<Segment<K, V>>):
            List<Segment<K, V>> {

        logger.debug { "Triggering a compact operation" }
        val compactedSegments = LinkedList<Segment<K, V>>()

        var segCounter = 1
        // TODO compact according to segment size instead.
        val memorySegment = CompactedLSMSegment(segmentFactory, segmentThreshold, keySize, valueSize)
        for (segment in segmentsToCompact) {

            logger.trace { "Compacting segment ${segCounter++}/${compactedSegments.size}" }
            segment.logKV.useEntries { entries ->
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

private class CompactedLSMSegment<K, V>(private val segmentFactory: SegmentFactory<K, V>,
                                        private val segmentThreshold: Long,
                                        private val keySize: SizeCalculator<K>,
                                        private val valueSize: SizeCalculator<V>) {

    private val compactedContent = mutableMapOf<K, V>()
    private var size: Long = 0L

    fun upsert(entry: Map.Entry<K, V>) {
        // TODO we might want to prune tombstones to optimize storage, or leave this as is to optimize reads of
        //      deleted keys
        val valueSize: Int = compactedContent[entry.key]
            ?.let { valueSize.sizeOf(it) }
            ?: 0
        compactedContent[entry.key] = entry.value
        size += keySize.sizeOf(entry.key)
        size += this.valueSize.sizeOf(entry.value) - valueSize
    }

    fun flush(): Segment<K, V> {
        val compacted = segmentFactory.createSegment()

        compacted.logKV.appendAll(compactedContent)
        compactedContent.clear()

        return compacted
    }

    fun isFull() = compactedContent.size >= segmentThreshold

}
