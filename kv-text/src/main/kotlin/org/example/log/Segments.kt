package org.example.log

import java.nio.file.Files
import java.nio.file.Path
import java.util.Collections.binarySearch
import kotlin.math.abs

internal class Segments(path: Path, private val segmentSize: Long): Iterable<SingleFileLog> {

    private val factory = SegmentFactory(path)

    private var offsets = mutableListOf(0L)
    private var segments = mutableListOf(factory.createSegment())

    fun from(offset: Long): List<Log> {

        if (offset == 0L) {
            return segments
        }

        val binarySearch = binarySearch(offsets, offset)
        val targetIndex = if (binarySearch < 0) abs(binarySearch) - 2 else binarySearch

        val targetOffset = offsets[targetIndex]
        if (targetOffset == offset) {
            return segments.subList(targetIndex, segments.size)
        }
        val subSegment = SubSegmentLog(offset - targetOffset, segments[targetIndex])

        if (targetIndex == segments.size - 1) {
            return listOf(subSegment)
        }

        return listOf(subSegment) + segments.subList(targetIndex + 1, segments.size)
    }

    fun closedOffset() = offsets.last()

    fun openSegment(): Log {

        val lastSegment = segments.last()

        if (lastSegment.len >= segmentSize) {
            // TODO add log
            offsets.add(offsets.last() + lastSegment.len)
            val openSegment = factory.createSegment()
            segments.add(openSegment)
            return openSegment
        }
        // TODO add log
        return lastSegment
    }

    override fun iterator() = segments.iterator()

    fun <K> compact(selector: (String) -> K) {

        val segmentsToCompact = segments.subList(0, segments.size - 1)
        val compactedSegments = mutableListOf<SingleFileLog>()
        val newOffsets = mutableListOf(0L)

        for (segment in segmentsToCompact) {
            val compactedSegment = factory.createSegment()
            segment.lines()
                    .distinctBy(selector)
                    .forEach { compactedSegment.append(it) }
            if (compactedSegments.isNotEmpty()) {
                newOffsets.add(newOffsets.last() + compactedSegments.last().len)
            }
            compactedSegments.add(compactedSegment)
        }
        compactedSegments.addAll(segments.subList(compactedSegments.size, segments.size))
        segments = compactedSegments
        offsets = newOffsets
    }
}

private class SegmentFactory(private val path: Path, private var segmentCounter: Int = 0) {

    fun createSegment() = path.resolve("segment_${segmentCounter++}")
            .also { Files.createFile(it) }
            .let { SingleFileLog(it) }
}

private class SubSegmentLog(private val offset: Long, private val log: Log): Log {

    override fun append(line: String): Long = log.append(line)

    override fun appendAll(lines: List<String>): List<Long> = log.appendAll(lines)

    override fun lines(offset: Long): Sequence<String> = log.lines(this.offset + offset)

    override fun linesWithOffset(offset: Long): Sequence<Pair<Long, String>>
            = log.linesWithOffset(this.offset + offset)
}
