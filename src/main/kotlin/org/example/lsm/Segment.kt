package org.example.lsm

import org.example.kv.KeyValueStore
import org.example.log.Log
import org.example.log.LogFactory
import java.nio.file.Files.createFile
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

class Segment<K, V> (
    val log: Log<Map.Entry<K, V>>,
    private val segmentThreshold: Long) {

    fun isFull(): Boolean = log.size() >= segmentThreshold

    fun clear() = log.clear()

    val size: Long
    get() = log.size()
}

interface OpenSegment<K, V>: KeyValueStore<K, V> {

    fun closeSegment(): Segment<K, V>

    fun isFull(): Boolean
}

abstract class SegmentFactory<K, V>(
    private val directory: SegmentDirectory,
    private val logFactory: LogFactory<Map.Entry<K, V>>,
    val segmentThreshold: Long = 1024 * 1024,
)  {

    fun createSegment(): Segment<K, V> = directory
        .createSegmentFile()
        .let { logFactory.create(it) }
        .let { Segment(it, segmentThreshold) }

    abstract fun createOpenSegment(): OpenSegment<K, V>

}

class SegmentDirectory(private val path: Path,
                       private val segCounter: AtomicInteger = AtomicInteger()
) {

    fun createSegmentFile(): Path = segCounter
        .getAndIncrement()
        .toString()
        .let { path.resolve(it) }
        .let { createFile(it) }
}
