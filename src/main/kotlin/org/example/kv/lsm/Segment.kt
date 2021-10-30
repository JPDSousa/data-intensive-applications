package org.example.kv.lsm

import org.example.kv.LogKeyValueStore
import org.example.kv.LogKeyValueStoreFactory
import org.example.kv.TombstoneKeyValueStore
import org.example.log.LogFactory
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.createFile
import kotlin.io.path.notExists
import kotlin.streams.asSequence

class Segment<K, V> (
    val logKV: LogKeyValueStore<K, V>,
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

interface OpenSegmentFactory<K, V> {

    fun createOpenSegment(): OpenSegment<K, V>
}

open class SegmentManager<K, V>(
    private val openSegmentFactory: OpenSegmentFactory<K, V>,
    private val directory: SegmentDirectory,
    private val logFactory: LogFactory<Map.Entry<K, V>>,
    private val logKVFactory: LogKeyValueStoreFactory<K, V>,
    private val mergeStrategy: SegmentMergeStrategy<K, V>,
    private val segmentThreshold: Long): OpenSegmentFactory<K, V> by openSegmentFactory  {

    // TODO this is not accounting for extra files required by the segment
    internal fun loadClosedSegments(): ClosedSegments<K, V> = directory.loadSegmentFiles { files ->
        files.map { logFactory.create(it) }
        .map { logKVFactory.createFromPair(it) }
        // TODO load segmentThreshold from file
        .map { Segment(it, segmentThreshold) }
        .toCollection(LinkedList())
        .let { ClosedSegments(mergeStrategy, it) }
    }


}

class SegmentFactory<K, V>(
    private val directory: SegmentDirectory,
    private val logFactory: LogFactory<Map.Entry<K, V>>,
    private val logKVFactory: LogKeyValueStoreFactory<K, V>,
    private val segmentThreshold: Long,
    private val idGenerator: AtomicInteger = AtomicInteger()
) {

    fun createSegment(): Segment<K, V> = directory
        .createSegmentFile(idGenerator.getAndIncrement())
        .let { logFactory.create(it) }
        .let { logKVFactory.createFromPair(it) }
        .let { Segment(it, segmentThreshold) }

}

class SegmentDirectory(private val path: Path) {

    // 1. Segment files should be sorted by causality
    // 2. This class should be resilient to dangling compacted segment files
    // 3. Some segments might require additional files (e.g., unsorted logs might persist hash indices)

    // TODO sort segments according to causality
    fun <R> loadSegmentFiles(block: (Sequence<Path>) -> R): R = Files.list(path).use {
        block(it.asSequence().filter { it.fileName.startsWith("seg-") })
    }

    fun createSegmentFile(segmentId: Int): Path = segmentId.toString()
        .let { "seg-$it" }
        .let { Paths.get(it) }
        .let { createFile(it) }

    fun createFile(fileName: Path, allowExisting: Boolean = false): Path = path.resolve(fileName)
        .also {
            if (allowExisting == it.notExists()) {
                it.createFile()
            }
        }
}
