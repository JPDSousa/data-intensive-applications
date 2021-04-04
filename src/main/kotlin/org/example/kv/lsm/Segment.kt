package org.example.kv.lsm

import org.example.kv.LogBasedKeyValueStore
import org.example.kv.LogBasedKeyValueStoreFactory
import org.example.kv.TombstoneKeyValueStore
import org.example.log.Log
import org.example.log.LogFactory
import java.nio.file.Files
import java.nio.file.Files.createFile
import java.nio.file.Path
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.streams.asSequence

class Segment<K, V> (
    val logKV: LogBasedKeyValueStore<K, V>,
    private val segmentThreshold: Long) {

    fun isFull(): Boolean = size >= segmentThreshold

    fun clear() = logKV.clear()

    val size: Long
    get() = logKV.size
}

interface OpenSegment<K, V>: TombstoneKeyValueStore<K, V> {

    fun closeSegment(): Segment<K, V>

    fun isFull(): Boolean
}

abstract class SegmentManager<K, V>(
    private val directory: SegmentDirectory,
    private val logFactory: LogFactory<Map.Entry<K, V>>,
    private val logKVFactory: LogBasedKeyValueStoreFactory<K, V>,
    private val mergeStrategy: SegmentMergeStrategy<K, V>,
    private val segmentThreshold: Long)  {

    // TODO this is not accounting for extra files required by the segment
    internal fun loadClosedSegments(): ClosedSegments<K, V> = directory.loadSegmentFiles()
        .map { logFactory.create(it) }
        .map { logKVFactory.createFromPair(it) }
        // TODO load segmentThreshold from file
        .map { Segment(it, segmentThreshold) }
        .toCollection(LinkedList())
        .let { ClosedSegments(mergeStrategy, it) }

    abstract fun createOpenSegment(): OpenSegment<K, V>

}

class SegmentFactory<K, V>(private val directory: SegmentDirectory,
                           private val logFactory: LogFactory<Map.Entry<K, V>>,
                           private val logKVFactory: LogBasedKeyValueStoreFactory<K, V>,
                           private val segmentThreshold: Long) {

    fun createSegment(): Segment<K, V> = directory
        .createSegmentFile()
        .let { logFactory.create(it) }
        .let { logKVFactory.createFromPair(it) }
        .let { Segment(it, segmentThreshold) }

}

class SegmentDirectory(private val path: Path,
                       private val segCounter: AtomicInteger = AtomicInteger()
) {

    // 1. Segment files should be sorted by causality
    // 2. This class should be resilient to dangling compacted segment files
    // 3. Some segments might require additional files (e.g., unsorted logs might persist hash indices)

    // TODO sort segments according to causality
    fun loadSegmentFiles(): Sequence<Path> = Files.list(path)
        .asSequence()
        .filter { it.fileName.startsWith("seg-") }

    fun createSegmentFile(): Path = segCounter
        .getAndIncrement()
        .toString()
        .let { "seg-$it" }
        .let { path.resolve(it) }
        .let { createFile(it) }
}
