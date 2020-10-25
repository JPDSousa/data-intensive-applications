package org.example.log

import kotlinx.serialization.Serializable

interface Index<K> {

    fun putOffset(key: K, offset: Long)

    fun putAllOffsets(pairs: Iterable<IndexEntry<K>>) {
        pairs.forEach {
            putOffset(it.key, it.offset)
        }
    }

    fun getOffset(key: K): Long?

    fun entries(): Collection<IndexEntry<K>>

}

@Serializable
data class IndexEntry<K>(val key: K, val offset: Long)
