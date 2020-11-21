package org.example.kv

import mu.KotlinLogging
import org.example.log.Index
import org.example.log.Log
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

internal class Segments<E, K, V>(private val factory: SegmentFactory<E, K, V>,
                                 private val segmentSize: Long): Iterable<IndexedKeyValueStore<E, K, V>> {

    private val logger = KotlinLogging.logger {}

    private val lock = ReentrantReadWriteLock()
    private var offsets = LinkedList(mutableListOf(0L))
    private var segments = LinkedList(listOf(factory.createSegment()))

    private fun closedSegments(): List<IndexedKeyValueStore<E, K, V>> = lock.read {

        val isFirstClosed = segments.first.isClosed()

        if (segments.size == 1) {
            return@read if (isFirstClosed) listOf(segments.first) else emptyList()
        }

        return@read if (isFirstClosed) segments else LinkedList(segments.subList(1, segments.size))
    }

    fun clear() = lock.write {
        segments.forEach { it.clear() }
        segments.clear()
        offsets.clear()
    }

    private fun closedOffset() = lock.read { offsets.first }

    fun openSegment(): KeyValueStore<K, V> {

        val firstSegment = lock.read { segments.first }

        if (firstSegment.log.size() >= segmentSize) {
            return lock.write {
                val actualFirstSegment = segments.first

                // now that we're inside an exclusive lock, we need to check again if we still need to open a new
                // segment
                val actualSize = actualFirstSegment.log.size()
                if (actualSize >= segmentSize) {

                    logger.debug { "Last segment is full. Closing and opening a new one." }
                    offsets.addFirst(closedOffset() + actualSize)

                    val openSegment = factory.createSegment()
                    segments.addFirst(openSegment)

                    logger.debug { "New open segment declared." }
                    return@write openSegment
                }
                return@write actualFirstSegment
            }

        }
        return firstSegment
    }

    internal fun compact() {

        logger.debug { "Triggering a compact operation" }
        val segmentsToCompact = closedSegments()
        val compactedSegments = LinkedList<IndexedKeyValueStore<E, K, V>>()
        val newOffsets = LinkedList<Long>()
        newOffsets.add(0L)

        for (segment in segmentsToCompact) {
            // TODO compact according to segment size instead.
            val memorySegment = CompactedSegment(factory, 1000)

            segment.forEachEntry { entry ->
                memorySegment.upsert(entry)
                if (memorySegment.isFull()) {
                    val compactedSegment = memorySegment.flush()

                    if (compactedSegments.isNotEmpty()) {
                        newOffsets.add(newOffsets.last() + compactedSegments.last().log.size())
                    }
                    compactedSegments.add(compactedSegment)
                }
            }

            if (memorySegment.isNotEmpty()) {
                // TODO copy paste from above
                val compactedSegment = memorySegment.flush()

                if (compactedSegments.isNotEmpty()) {
                    newOffsets.add(newOffsets.last() + compactedSegments.last().log.size())
                }
                compactedSegments.add(compactedSegment)
            }
        }

        lock.write {
            compactedSegments.addAll(segments.subList(segmentsToCompact.size, segments.size))
            segments = compactedSegments
            newOffsets.addAll(offsets.subList(segmentsToCompact.size, offsets.size))
            offsets = newOffsets
        }

        segmentsToCompact.forEach { it.clear() }
    }

    private fun IndexedKeyValueStore<E, K, V>.isClosed() = log.size() >= segmentSize

    override fun iterator() = segments.iterator()
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

// Thread safe
internal class SegmentFactory<E, K, V>(private val resourceFactory: SegmentResourcesFactory<E, K, V>,
                                       private var segmentCounter: AtomicInteger = AtomicInteger(0)) {

    fun createSegment(): IndexedKeyValueStore<E, K, V> = resourceFactory.createKeyValueStore(
            segmentCounter.getAndIncrement().toString())

}

private class CompactedSegment<E, K, V>(private val factory: SegmentFactory<E, K, V>,
                                        private val limit: Int) {

    private val compactedContent = mutableMapOf<K, V>()

    fun upsert(entry: Pair<K, V>) = compactedContent.put(entry.first, entry.second)

    fun flush(): IndexedKeyValueStore<E, K, V> {
        val compacted = factory.createSegment()

        compacted.putAll(compactedContent)
        compactedContent.clear()

        return compacted
    }

    fun isFull() = compactedContent.size >= limit

    fun isNotEmpty() = compactedContent.isNotEmpty()

}
