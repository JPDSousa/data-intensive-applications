package org.example.kv

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.example.lsm.LSMStructure
import org.example.lsm.SegmentFactory
import java.util.concurrent.Executors

internal class LSMKeyValueStore<E, K, V>(segmentFactory: SegmentFactory<IndexedKeyValueStore<E, K, V>>,
                                         private val tombstone: V,
                                         segmentSize: Long = 1024 * 1024,
                                         private val compactCycle: Long = 1000): KeyValueStore<K, V> {

    private val segments = LSMStructure(segmentFactory, segmentSize, KeyValueLogMergeStrategy(segmentFactory))
    @Volatile
    private var opsWithoutCompact: Long = 0

    init {
        val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
        GlobalScope.launch(dispatcher) {
            while (true) {
                delay(1000 * 60)
                val opsWithoutCompact = this@LSMKeyValueStore.opsWithoutCompact
                if (opsWithoutCompact >= compactCycle) {
                    segments.compact()
                    // this is possibly not thread safe
                    this@LSMKeyValueStore.opsWithoutCompact -= opsWithoutCompact
                }
            }
        }
    }

    override fun put(key: K, value: V) {
        segments.openSegment().structure.put(key, value)
    }

    override fun get(key: K): V? {
        for (segment in segments) {
            val kvs = segment.structure
            val value = kvs.getWithTombstone(key)

            if (value == tombstone) {
                return null
            }

            if (value != null) {
                return value
            }
        }
        return null
    }

    override fun delete(key: K) {
        this.segments.openSegment().structure.delete(key)
    }

    override fun clear() = segments.clear()
}


