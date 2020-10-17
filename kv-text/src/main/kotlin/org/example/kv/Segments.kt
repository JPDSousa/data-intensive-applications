package org.example.kv

import mu.KotlinLogging
import org.example.index.CheckpointableIndex
import org.example.log.SingleFileLog
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files.createDirectories
import java.nio.file.Files.createFile
import java.nio.file.Path
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

internal class Segments(path: Path, private val segmentSize: Long): Iterable<TextKeyValueStore> {

    private val logger = KotlinLogging.logger {}

    private val factory = SegmentFactory(path)

    private val lock = ReentrantReadWriteLock()
    private var offsets = LinkedList(mutableListOf(0L))
    private var segments = LinkedList(listOf(factory.createSegment()))

    private fun closedSegments(): List<TextKeyValueStore> = lock.read {

        val isFirstClosed = segments.first.isClosed()

        if (segments.size == 1) {
            return@read if (isFirstClosed) listOf(segments.first) else emptyList()
        }

        return@read if (isFirstClosed) segments else LinkedList(segments.subList(1, segments.size))
    }

    private fun closedOffset() = lock.read { offsets.first }

    fun openSegment(): KeyValueStore {

        val firstSegment = lock.read { segments.first }

        if (firstSegment.size() >= segmentSize) {
            return lock.write {
                val actualFirstSegment = segments.first

                // now that we're inside an exclusive lock, we need to check again if we still need to open a new
                // segment
                val actualSize = actualFirstSegment.size()
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
        val compactedSegments = LinkedList<TextKeyValueStore>()
        val newOffsets = LinkedList<Long>()
        newOffsets.add(0L)

        for (segment in segmentsToCompact) {
            val memorySegment = CompactedSegment(factory, segmentSize)

            segment.useEntries { entries ->
                entries.forEach { entry ->
                    memorySegment.upsert(entry)
                    if (memorySegment.isFull()) {
                        val compactedSegment = memorySegment.flush()

                        if (compactedSegments.isNotEmpty()) {
                            newOffsets.add(newOffsets.last() + compactedSegments.last().size())
                        }
                        compactedSegments.add(compactedSegment)
                    }
                }
            }
            if (memorySegment.isNotEmpty()) {
                // TODO copy paste from above
                val compactedSegment = memorySegment.flush()

                if (compactedSegments.isNotEmpty()) {
                    newOffsets.add(newOffsets.last() + compactedSegments.last().size())
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

    private fun TextKeyValueStore.isClosed() = size() >= segmentSize

    override fun iterator() = segments.iterator()
}

private fun TextKeyValueStore.size() = this.log.size()

// Thread safe
private class SegmentFactory(private val path: Path,
                             val charset: Charset = UTF_8,
                             private var segmentCounter: AtomicInteger = AtomicInteger(0)) {

    fun createSegment(): TextKeyValueStore {
        val segmentId = segmentCounter.getAndIncrement()

        val segmentDir = createDirectories(path.resolve("segment_$segmentId"))
        val logPath = createFile(segmentDir.resolve("log"))

        val log = SingleFileLog(logPath)

        return TextKeyValueStore(
                CheckpointableIndex(segmentDir, log::size),
                log
        )
    }
}

private class CompactedSegment(private val factory: SegmentFactory,
                                  private val limit: Long) {

    private val compactedContent = mutableMapOf<String, String>()
    private var size = 0L

    fun upsert(entry: Pair<String, String>) {
        compactedContent.compute(entry.first) { _, oldValue ->
            val newValue = entry.second
            if (oldValue != null) {
                size -= (oldValue.byteLength() - newValue.byteLength())
            }
            return@compute newValue
        }
    }

    fun flush(): TextKeyValueStore {
        val compacted = factory.createSegment()

        compacted.putAll(compactedContent)
        compactedContent.clear()
        size = 0L

        return compacted
    }

    private fun String.byteLength() = this.toByteArray(factory.charset).size

    fun isFull() = size >= limit

    fun isNotEmpty() = size > 0

}
