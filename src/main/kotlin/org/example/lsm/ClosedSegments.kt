package org.example.lsm

import mu.KotlinLogging
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class ClosedSegments<K, V>(private val mergeStrategy: SegmentMergeStrategy<K, V>): Iterable<Segment<K, V>> {

    private val logger = KotlinLogging.logger {}

    private val lock = ReentrantReadWriteLock()
    private var segments = LinkedList<Segment<K, V>>()

    fun accept(openSegment: OpenSegment<K, V>) {

        logger.trace { "Closing segment" }
        val segment = openSegment.closeSegment()

        lock.write {
            logger.trace { "Adding segment closed segments" }
            segments.add(segment)
        }
    }

    internal fun compact() {

        lock.read {
            val segmentsToCompact = LinkedList(segments)
            logger.debug { "Triggering a compact operation" }
            val compactedSegments = LinkedList(mergeStrategy.merge(segmentsToCompact))

            lock.write {
                segments = compactedSegments
            }

            logger.debug { "Clearing dangling segments" }
            segmentsToCompact.forEach { it.clear() }
        }
    }

    fun clear() = lock.write {
        segments.forEach { it.clear() }
        segments.clear()
    }

    override fun iterator(): Iterator<Segment<K, V>> = lock.read { segments.iterator() }
}
