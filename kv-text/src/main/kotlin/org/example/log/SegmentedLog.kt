package org.example.log

import java.nio.file.Path
import java.util.Collections.binarySearch
import kotlin.math.abs

class SegmentedLog(path: Path,
                   // 1 MB
                   segmentSize: Long = 1024 * 1024): Log {

    private val factory = SegmentFactory(path)
    private val segments = Segments(factory, segmentSize)

    override fun append(line: String): Long {
        val openSegment = this.segments.openSegment()
        // openSegment() must be called before closedOffset, as that method might close another segment
        return segments.closedOffset() + openSegment.append(line)
    }

    override fun appendAll(lines: List<String>): List<Long> = lines.map { append(it) }

    override fun lines(offset: Long) = segments.from(offset)
            .flatMap { it.lines() }
            .asSequence()

    override fun linesWithOffset(offset: Long) = segments.from(offset)
            .flatMap { it.linesWithOffset() }
            .asSequence()
}

private class SegmentFactory(private val path: Path, private var segmentCounter: Int = 0) {

    fun createSegment() = SingleFileLog(path.resolve("segment_${segmentCounter++}"))
}

private class SubSegmentLog(private val offset: Long, private val log: Log): Log {

    override fun append(line: String): Long = log.append(line)

    override fun appendAll(lines: List<String>): List<Long> = log.appendAll(lines)

    override fun lines(offset: Long): Sequence<String> = log.lines(this.offset + offset)

    override fun linesWithOffset(offset: Long): Sequence<Pair<Long, String>> = log.linesWithOffset(this.offset +
            offset)
}

private class Segments(private val factory: SegmentFactory, private val segmentSize: Long): Iterable<SingleFileLog> {

    private val offsets = mutableListOf(0L)
    private val segments = mutableListOf(factory.createSegment())

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
}
