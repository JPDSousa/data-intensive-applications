package org.example.index

import kotlinx.serialization.Serializable
import org.example.concepts.ClearMixin
import org.example.concepts.ImmutableDictionaryMixin
import org.example.concepts.MutableDictionaryMixin

interface Index<K>: ImmutableDictionaryMixin<K, Long>, MutableDictionaryMixin<K, Long>, ClearMixin {

    fun putAllOffsets(pairs: Iterable<IndexEntry<K>>) {
        pairs.forEach {
            this[it.key] = it.offset
        }
    }

    fun <R> useEntries(block: (Sequence<IndexEntry<K>>) -> R): R

}

@Serializable
data class IndexEntry<K>(val key: K, val offset: Long)

interface IndexFactory<K> {

    fun create(indexName: String): Index<K>
}
