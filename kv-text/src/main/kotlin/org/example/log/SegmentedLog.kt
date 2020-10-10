package org.example.log

import java.nio.file.Path
import java.util.*

class SegmentedLog(path: Path,
                   // 1 MB
                   private val segmentSize: Long = 1024 * 1024): Log {

    private val factory = SegmentFactory(path)
    private val segments = mutableListOf(factory.createSegment())
    private var closedOffset = 0L

    private fun openSegment(): Log {

        val lastSegment = segments.last()

        if (lastSegment.len >= segmentSize) {
            // TODO add log
            closedOffset += lastSegment.len
            val openSegment = factory.createSegment()
            segments.add(openSegment)
            return openSegment
        }
        // TODO add log
        return lastSegment
    }

    private fun segmentsFrom(offset: Long): List<Log> {

        var remaining = offset
        val segments = LinkedList<Log>()

        for (segment in this.segments) {
            if (remaining <= 0L) {
                segments.add(segment)
            } else if (remaining < segment.len) {
                segments.add(SubSegmentLog(remaining, segment))
            }
            // if remaining is bigger then len then we don't want this segment
            remaining -= segment.len
        }
        return segments
    }

    override fun append(line: String): Long {
        val openSegment = openSegment()
        // openSegment() must be called before closedOffset, as that method updates the variable
        return closedOffset + openSegment.append(line)
    }

    override fun appendAll(lines: List<String>): List<Long> = lines.map { append(it) }

    override fun lines(offset: Long): Sequence<String> = segmentsFrom(offset)
            .flatMap { it.lines() }
            .asSequence()

    override fun linesWithOffset(offset: Long): Sequence<Pair<Long, String>> = segmentsFrom(offset)
            .flatMap { it.linesWithOffset() }
            .asSequence()
}

private class SegmentFactory(private val path: Path, private var segmentCounter: Int = 0) {

    fun createSegment() = SingleFileLog(path.resolve("segment_${segmentCounter++}"))
}

private class SubSegmentLog(private val offset: Long, private val log: Log): Log {

    override fun append(line: String): Long = log.append(line)

    override fun appendAll(lines: List<String>): List<Long> = log.appendAll(lines)

    override fun lines(offset: Long): Sequence<String> = lines(this.offset + offset)

    override fun linesWithOffset(offset: Long): Sequence<Pair<Long, String>> = linesWithOffset(this.offset + offset)
}
