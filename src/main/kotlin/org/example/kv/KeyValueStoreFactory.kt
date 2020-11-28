package org.example.kv

import org.example.kv.lsm.LSMKeyValueStore
import org.example.index.Index
import java.nio.file.Path

class KeyValueStoreFactory {

    fun createStringKeyValueStore(index: Index<String>,
                                  logKV: LogBasedKeyValueStore<String, String>): KeyValueStore<String, String> {
        return IndexedKeyValueStore(
                index,
                tombstoneStr,
                logKV
        )
    }

    fun createBinaryKeyValueStore(
            index: Index<ByteArray>,
            logKV: LogBasedKeyValueStore<ByteArray, ByteArray>): KeyValueStore<ByteArray, ByteArray> {

        return IndexedKeyValueStore(
                index,
                tombstoneByte,
                logKV
        )
    }

    fun createSegmentedStringKeyValueStore(kvDir: Path): KeyValueStore<String, String> = LSMKeyValueStore(
            segmentSize,
            tombstoneStr
    )

    fun createSegmentedBinaryKeyValueStore(kvDir: Path): KeyValueStore<ByteArray, ByteArray> = LSMKeyValueStore(
            segmentSize,
            tombstoneByte
    )

    companion object {

        private const val segmentSize: Long = 1024 * 1024

    }
}

