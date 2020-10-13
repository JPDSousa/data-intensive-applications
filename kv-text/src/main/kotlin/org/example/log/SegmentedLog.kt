package org.example.log

import java.nio.file.Path

class SegmentedLog(path: Path,
                   // 1 MB
                   segmentSize: Long = 1024 * 1024): Log {

    private val segments = Segments(path, segmentSize)

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
