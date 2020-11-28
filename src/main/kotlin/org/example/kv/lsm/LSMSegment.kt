package org.example.kv.lsm

import org.example.kv.IndexedKeyValueStore
import org.example.kv.LogBasedKeyValueStore
import org.example.kv.LogBasedKeyValueStoreFactory
import org.example.index.Index
import org.example.log.LogFactory
import org.example.lsm.Segment
import org.example.lsm.SegmentFactory
import java.nio.file.Files.createFile
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

internal class LSMSegment<K, V>(override val structure: LogBasedKeyValueStore<K, V>,
                                private val segmentThreshold: Long) : Segment<LogBasedKeyValueStore<K, V>> {

    override fun isClosed(): Boolean = structure.isClosed()

    override fun clear() = structure.clear()

    override val size: Long
        get() = structure.log.size()

    private fun LogBasedKeyValueStore<K, V>.isClosed() = log.size() >= segmentThreshold
}

internal class LSMSegmentFactory<E, K, V>(private val kvDir: Path,
                                          private val logFactory: LogFactory<E>,
                                          private val kvFactory: LogBasedKeyValueStoreFactory<E, K, V>,
                                          private val segmentThreshold: Long,
                                          private var segmentCounter: AtomicInteger = AtomicInteger(0)):
        SegmentFactory<LogBasedKeyValueStore<K, V>> {

    override fun createSegment(): Segment<LogBasedKeyValueStore<K, V>> = segmentCounter.getAndIncrement().toString()
            .let { kvDir.resolve(it) }
            .let { createFile(it) }
            .let { logFactory.create(it) }
            .let { kvFactory.create(it) }
            .let { LSMSegment(it, segmentThreshold) }
}

internal interface SegmentResourcesFactory<K, V> {

    fun createIndex(segmentId: String): Index<K>

    fun createLogBasedKeyValueStore(segmentId: String): LogBasedKeyValueStore<K, V>

    val tombstone: V

    fun createKeyValueStore(segmentId: String) = IndexedKeyValueStore(
            createIndex(segmentId),
            tombstone,
            createLogBasedKeyValueStore(segmentId)
    )

}
