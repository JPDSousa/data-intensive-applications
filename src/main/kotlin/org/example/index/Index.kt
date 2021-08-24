package org.example.index

import kotlinx.serialization.Serializable

interface Index<K> {

    fun putOffset(key: K, offset: Long)

    fun putAllOffsets(pairs: Iterable<IndexEntry<K>>) {
        pairs.forEach {
            putOffset(it.key, it.offset)
        }
    }

    fun getOffset(key: K): Long?

    fun <R> useEntries(block: (Sequence<IndexEntry<K>>) -> R): R

    fun clear()

}

@Serializable
data class IndexEntry<K>(val key: K, val offset: Long)

interface IndexFactory<K> {

    fun create(indexName: String): Index<K>
}
