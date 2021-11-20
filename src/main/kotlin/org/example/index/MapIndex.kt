package org.example.index

import org.example.concepts.ImmutableDictionaryMixin
import org.example.concepts.MutableDictionaryMixin
import org.example.concepts.asImmutableDictionaryMixin
import org.example.concepts.asMutableDictionaryMixin
import org.koin.core.qualifier.named
import java.util.*

val treeIndexQ = named("treeIndex")
val hashIndexQ = named("hashIndex")
val indicesQ = listOf(treeIndexQ, hashIndexQ)

private class MapIndex<K>(
    private val index: MutableMap<K, Long>
): Index<K>,
    MutableDictionaryMixin<K, Long> by index.asMutableDictionaryMixin(),
    ImmutableDictionaryMixin<K, Long> by index.asImmutableDictionaryMixin() {

    override fun putAllOffsets(pairs: Iterable<IndexEntry<K>>) {
        pairs.forEach { put(it.key, it.offset) }
    }

    override fun <R> useEntries(block: (Sequence<IndexEntry<K>>) -> R) = index
        .map { IndexEntry(it.key, it.value) }
        .asSequence()
        .let(block)

}

class TreeIndexFactory<K: Comparable<K>>: IndexFactory<K> {
    override fun create(indexName: String): Index<K> = MapIndex(TreeMap())
}

class HashIndexFactory<K>: IndexFactory<K> {
    override fun create(indexName: String): Index<K> = MapIndex(HashMap())
}
