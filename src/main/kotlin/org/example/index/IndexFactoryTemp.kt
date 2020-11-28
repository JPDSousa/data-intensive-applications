package org.example.index

import kotlinx.serialization.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.protobuf.ProtoBuf
import org.example.log.Log
import org.example.log.LogEncoder

class IndexFactoryTemp {

    fun <K : Comparable<K>> createTreeIndex(log: Log<IndexEntry<K>>? = null): Index<K> {

        val baseIndex = TreeIndex<K>()
        if (log == null) {
            return baseIndex
        }

        return CheckpointableIndex(baseIndex, log)
    }

    fun <K> createHashIndex(log: Log<IndexEntry<K>>? = null) : Index<K> {

        val baseIndex = HashIndex<K>()
        if (log == null) {
            return baseIndex
        }

        return CheckpointableIndex(baseIndex, log)
    }

    fun <K> createStringLogEncoder(log: Log<String>): Log<IndexEntry<K>> = LogEncoder(
            log,
            { Json.encodeToString(it)},
            { Json.decodeFromString(it) }
    )

    @ExperimentalSerializationApi
    fun <K> createBinaryLogEncoder(log: Log<ByteArray>): Log<IndexEntry<K>> = LogEncoder(
            log,
            { ProtoBuf.encodeToByteArray(it) },
            { ProtoBuf.decodeFromByteArray(it) }
    )

}
