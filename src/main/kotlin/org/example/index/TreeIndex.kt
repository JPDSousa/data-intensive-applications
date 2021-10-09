package org.example.index

import org.koin.core.qualifier.named
import java.util.*

val treeIndexQ = named("treeIndex")

private class TreeIndex<K: Comparable<K>>(private val index: MutableMap<K, Long> = TreeMap<K, Long>()): Index<K> {

    override fun putOffset(key: K, offset: Long) {
        index[key] = offset
    }

    override fun putAllOffsets(pairs: Iterable<IndexEntry<K>>) {
        pairs.forEach { index[it.key] = it.offset }
    }

    override fun getOffset(key: K) = index[key]

    override fun <R> useEntries(block: (Sequence<IndexEntry<K>>) -> R) = index
        .map { IndexEntry(it.key, it.value) }
        .asSequence()
        .let(block)

    override fun clear() {
        index.clear()
    }
}

class TreeIndexFactory<K: Comparable<K>>: IndexFactory<K> {

    override fun create(indexName: String): Index<K> = TreeIndex()

}
