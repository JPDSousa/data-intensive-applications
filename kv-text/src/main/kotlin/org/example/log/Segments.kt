package org.example.log

import mu.KotlinLogging
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files.createFile
import java.nio.file.Path
import java.util.Collections.binarySearch
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.math.abs

internal class Segments(path: Path, private val segmentSize: Long) {

    private val logger = KotlinLogging.logger {}

    private val factory = SegmentFactory(path)

    private val lock = ReentrantReadWriteLock()
    private var offsets = mutableListOf(0L)
    private var segments = mutableListOf(factory.createSegment())

    fun from(offset: Long): List<Log> {

        if (offset == 0L) {
            return segments
        }

        return lock.read {
            val binarySearch = binarySearch(offsets, offset)
            val targetIndex = if (binarySearch < 0) abs(binarySearch) - 2 else binarySearch

            val targetOffset = offsets[targetIndex]
            if (targetOffset == offset) {
                return@read segments.subList(targetIndex, segments.size)
            }
            val subSegment = SubSegmentLog(offset - targetOffset, segments[targetIndex])

            if (targetIndex == segments.size - 1) {
                return@read listOf(subSegment)
            }

            return@read (listOf(subSegment) + segments.subList(targetIndex + 1, segments.size))
        }
    }

    private fun closedSegments() = lock.read { ArrayList(segments.subList(0, segments.size - 1)) }

    fun closedOffset() = lock.read { offsets.last() }

    fun openSegment(): Log {

        val lastSegment = lock.read { segments.last() }

        if (lastSegment.len >= segmentSize) {
            return lock.write {
                val actualLastSegment = segments.last()

                // now that we're inside an exclusive lock, we need to check again if we still need to open a new
                // segment
                if (actualLastSegment.len >= segmentSize) {
                    // TODO add log
                    offsets.add(closedOffset() + actualLastSegment.len)
                    val openSegment = factory.createSegment()
                    segments.add(openSegment)
                    return@write openSegment
                }
                return@write actualLastSegment
            }

        }
        // TODO add log
        return lastSegment
    }

    fun <K> compact(selector: (String) -> K) {

        logger.error { "Triggering a compact operation" }
        val segmentsToCompact = closedSegments()
        val compactedSegments = ArrayList<SingleFileLog>(segmentsToCompact.size)
        val newOffsets = ArrayList<Long>(segmentsToCompact.size)
        newOffsets.add(0L)

        for (segment in segmentsToCompact) {
            val memorySegment = CompactedSegment(factory, selector, segmentSize)

            segment.useLines { sequence ->
                sequence.forEach { newValue ->
                    memorySegment.upsert(newValue)
                    if (memorySegment.isFull()) {
                        val compactedSegment = memorySegment.flush()

                        if (compactedSegments.isNotEmpty()) {
                            newOffsets.add(newOffsets.last() + compactedSegments.last().len)
                        }
                        compactedSegments.add(compactedSegment)
                    }
                }
            }
            if (memorySegment.isNotEmpty()) {
                // TODO copy paste from above
                val compactedSegment = memorySegment.flush()

                if (compactedSegments.isNotEmpty()) {
                    newOffsets.add(newOffsets.last() + compactedSegments.last().len)
                }
                compactedSegments.add(compactedSegment)
            }
        }
        lock.write {
            compactedSegments.addAll(segments.subList(compactedSegments.size, segments.size))
            segments = compactedSegments
            newOffsets.addAll(offsets.subList(newOffsets.size, offsets.size))
            offsets = newOffsets
        }
    }
}

// Thread safe
private class SegmentFactory(private val path: Path,
                             val charset: Charset = UTF_8,
                             private var segmentCounter: AtomicInteger = AtomicInteger(0)) {

    fun createSegment() = path.resolve("segment_${segmentCounter.getAndIncrement()}")
            .also { createFile(it) }
            .let { SingleFileLog(it, charset) }
}

private class SubSegmentLog(private val offset: Long, private val log: Log): Log {

    override fun append(line: String): Long = log.append(line)

    override fun appendAll(lines: Collection<String>): Collection<Long> = log.appendAll(lines)

    override fun <T> useLines(offset: Long, block: (Sequence<String>) -> T): T
            = log.useLines(this.offset + offset, block)

    override fun <T> useLinesWithOffset(offset: Long, block: (Sequence<LineWithOffset>) -> T): T
            = log.useLinesWithOffset(this.offset + offset, block)

}

private class CompactedSegment<K>(private val factory: SegmentFactory,
                                  private val selector: (String) -> K,
                                  private val limit: Long) {

    private val compactedContent = mutableMapOf<K, String>()
    private var size = 0L

    fun upsert(newValue: String) {
        compactedContent.compute(selector(newValue)) { _, oldValue ->
            if (oldValue != null) {
                size -= (oldValue.byteLength() - newValue.byteLength())
            }
            return@compute newValue
        }
    }

    fun flush(): SingleFileLog {
        val compacted = factory.createSegment()

        compacted.appendAll(compactedContent.values)
        compactedContent.clear()
        size = 0L

        return compacted
    }

    private fun String.byteLength() = this.toByteArray(factory.charset).size

    fun isFull() = size >= limit

    fun isNotEmpty() = size > 0

}
