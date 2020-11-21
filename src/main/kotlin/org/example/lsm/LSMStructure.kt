package org.example.lsm

import mu.KotlinLogging
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class LSMStructure<S>(private val factory: SegmentFactory<S>,
                      private val segmentThreshold: Long,
                      private val mergeStrategy: SegmentMergeStrategy<S>): Iterable<Segment<S>> {

    private val logger = KotlinLogging.logger {}

    private val lock = ReentrantReadWriteLock()
    private var segments = LinkedList(listOf(factory.createSegment()))

    private fun closedSegments(): List<Segment<S>> = lock.read {

        val isFirstClosed = segments.first.isClosed()

        if (segments.size == 1) {
            return@read if (isFirstClosed) listOf(segments.first) else emptyList()
        }

        return@read if (isFirstClosed) segments else LinkedList(segments.subList(1, segments.size))
    }

    fun clear() = lock.write {
        segments.forEach { it.clear() }
        segments.clear()
    }

    fun openSegment(): Segment<S> {

        val firstSegment = lock.read { segments.first }

        if (firstSegment.size >= segmentThreshold) {
            return lock.write {
                val actualFirstSegment = segments.first

                // now that we're inside an exclusive lock, we need to check again if we still need to open a new
                // segment
                val actualSize = actualFirstSegment.size
                if (actualSize >= segmentThreshold) {

                    logger.debug { "Last segment is full. Closing and opening a new one." }

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

        val segmentsToCompact = closedSegments()

        logger.debug { "Triggering a compact operation" }
        val compactedSegments = LinkedList(mergeStrategy.merge(segmentsToCompact))

        lock.write {
            compactedSegments.addAll(segments.subList(segmentsToCompact.size, segments.size))
            segments = compactedSegments
        }

        logger.debug { "Clearing dangling segments" }
        segmentsToCompact.forEach { it.clear() }
    }

    override fun iterator(): Iterator<Segment<S>> = segments.iterator()
}
