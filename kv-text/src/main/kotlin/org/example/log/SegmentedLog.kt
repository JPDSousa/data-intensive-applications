package org.example.log

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.nio.file.Path
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

class SegmentedLog(path: Path,
                   // 1 MB
                   segmentSize: Long = 1024 * 1024,
                   private val selector: (String) -> String): Log {

    private val compactThread = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

    private val opsPerCompact = segmentSize
    private val remainingTillCompact = AtomicLong(opsPerCompact)

    private val segments = Segments(path, segmentSize)

    override fun append(line: String): Long = runBlocking {
        remainingTillCompact.decrementAndGet()
        if (remainingTillCompact.compareAndSet(0, opsPerCompact)) {
            launch(compactThread) { segments.compact(selector) }
        }

        val openSegment = segments.openSegment()
        // openSegment() must be called before closedOffset, as that method might close another segment
        return@runBlocking segments.closedOffset() + openSegment.append(line)
    }

    override fun appendAll(lines: Collection<String>): Collection<Long> = lines.map { append(it) }

    override fun <T> useLines(offset: Long, block: (Sequence<String>) -> T): T = block(sequence {
        for (log in segments.from(offset)) {
            yieldAll(log.useLines { it.toList() })
        }
    })

    override fun <T> useLinesWithOffset(offset: Long, block: (Sequence<LineWithOffset>) -> T): T = block(sequence {
        for (log in segments.from(offset)) {
            yieldAll(log.useLinesWithOffset { it.toList() })
        }
    })

}
