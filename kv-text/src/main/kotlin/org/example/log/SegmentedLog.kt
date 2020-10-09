package org.example.log

import java.nio.file.Path

class SegmentedLog(path: Path,
                   // 1 MB
                   private val segmentSize: Long = 1024 * 1024): Log {

    private val factory = SegmentFactory(path)
    private val segments = mutableListOf(factory.createSegment())

    private fun openSegment(): Log {

        val lastSegment = segments.last()

        if (lastSegment.len >= segmentSize) {
            // TODO add log
            val openSegment = factory.createSegment()
            segments.add(openSegment)
            return openSegment
        }
        // TODO add log
        return lastSegment
    }

    private fun segmentOf(offset: Long): Pair<Log, Long>? {

        var remaining = offset
        for (segment in segments) {
            if (remaining <= segment.len) {
                return Pair(segment, remaining)
            }
            remaining -= segment.len
        }
        return null
    }

    override fun append(line: String): Long = openSegment().append(line)

    override fun appendAll(lines: List<String>): List<Long> = openSegment().appendAll(lines)

    override fun lines(offset: Long): Sequence<String> = segmentOf(offset)
            ?.let { it.first.lines(it.second) }
            ?: sequenceOf()

    override fun linesWithOffset(offset: Long): Sequence<Pair<Long, String>> = segmentOf(offset)
            ?.let { it.first.linesWithOffset(it.second) }
            ?: sequenceOf()
}

private class SegmentFactory(private val path: Path, private var segmentCounter: Int = 0) {

    fun createSegment() = SingleFileLog(path.resolve("segment_${segmentCounter++}"))
}
