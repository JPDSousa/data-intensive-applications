package org.example.kv

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.util.concurrent.Executors

internal class SegmentedKeyValueStore<E, K, V>(segmentFactory: SegmentFactory<E, K, V>,
                                               private val tombstone: V,
                                               segmentSize: Long = 1024 * 1024,
                                               private val compactCycle: Long = 1000): KeyValueStore<K, V> {

    private val segments = Segments(segmentFactory, segmentSize)
    @Volatile
    private var opsWithoutCompact: Long = 0

    init {
        val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
        GlobalScope.launch(dispatcher) {
            while (true) {
                delay(1000 * 60)
                val opsWithoutCompact = this@SegmentedKeyValueStore.opsWithoutCompact
                if (opsWithoutCompact >= compactCycle) {
                    segments.compact()
                    // this is possibly not thread safe
                    this@SegmentedKeyValueStore.opsWithoutCompact -= opsWithoutCompact
                }
            }
        }
    }

    override fun put(key: K, value: V) {
        segments.openSegment().put(key, value)
    }

    override fun get(key: K): V? {
        for (kvs in segments) {
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
        this.segments.openSegment().delete(key)
    }

    override fun clear() = segments.clear()
}


