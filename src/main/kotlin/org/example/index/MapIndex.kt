package org.example.index

import org.koin.core.qualifier.named
import java.util.*
import kotlin.collections.HashMap

val treeIndexQ = named("treeIndex")
val hashIndexQ = named("hashIndex")

private class MapIndex<K>(private val index: MutableMap<K, Long>): Index<K> {

    override fun put(key: K, value: Long) {
        index[key] = value
    }

    override fun putAllOffsets(pairs: Iterable<IndexEntry<K>>) {
        pairs.forEach { index[it.key] = it.offset }
    }

    override fun get(key: K) = index[key]

    override fun <R> useEntries(block: (Sequence<IndexEntry<K>>) -> R) = index
        .map { IndexEntry(it.key, it.value) }
        .asSequence()
        .let(block)

    override fun clear() {
        index.clear()
    }

    override fun delete(key: K) {
        index.remove(key)
    }
}

class TreeIndexFactory<K: Comparable<K>>: IndexFactory<K> {
    override fun create(indexName: String): Index<K> = MapIndex(TreeMap())
}

class HashIndexFactory<K>: IndexFactory<K> {
    override fun create(indexName: String): Index<K> = MapIndex(HashMap())
}
