package org.example.kv

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.nio.file.Path
import java.util.concurrent.Executors

class SegmentedKeyValueStore(path: Path,
                             segmentSize: Long = 1024 * 1024,
                             private val compactCycle: Long = 1000): KeyValueStore {

    private val kvs = Segments(path, segmentSize)
    @Volatile
    private var opsWithoutCompact: Long = 0

    init {
        val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
        GlobalScope.launch(dispatcher) {
            while (true) {
                delay(1000 * 60)
                val opsWithoutCompact = this@SegmentedKeyValueStore.opsWithoutCompact
                if (opsWithoutCompact >= compactCycle) {
                    kvs.compact()
                    // this is possibly not thread safe
                    this@SegmentedKeyValueStore.opsWithoutCompact -= opsWithoutCompact
                }
            }
        }
    }

    override fun put(key: String, value: String) {
        kvs.openSegment().put(key, value)
    }

    override fun get(key: String): String? {
        for (keyValueStore in kvs) {
            val value = keyValueStore.getWithTombstone(key)

            if (value == TextKeyValueStore.tombstone) {
                return null
            }

            if (value != null) {
                return value
            }
        }
        return null
    }

    override fun delete(key: String) {
        this.kvs.openSegment().delete(key)
    }
}


