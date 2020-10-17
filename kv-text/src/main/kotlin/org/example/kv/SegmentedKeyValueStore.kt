package org.example.kv

import java.nio.file.Path

class SegmentedKeyValueStore(path: Path, segmentSize: Long = 1024 * 1024): KeyValueStore {

    private val kvs = Segments(path, segmentSize)

    override fun put(key: String, value: String) {
        kvs.openSegment().put(key, value)
    }

    override fun get(key: String): String? {
        for (keyValueStore in kvs) {
            val value = keyValueStore.get(key)
            if (value != null) {
                return value
            }
        }
        return null
    }
}


