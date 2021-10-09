package org.example.index

import org.koin.core.qualifier.named

val hashIndexQ = named("hashIndex")

private class HashIndex<K>(private val index: MutableMap<K, Long> = HashMap()): Index<K> {

    override fun putOffset(key: K, offset: Long) {
        index[key] = offset
    }

    override fun getOffset(key: K): Long? {
        return index[key]
    }

    override fun putAllOffsets(pairs: Iterable<IndexEntry<K>>) {
        pairs.forEach { index[it.key] = it.offset }
    }

    override fun <R> useEntries(block: (Sequence<IndexEntry<K>>) -> R) = index
        .map { IndexEntry(it.key, it.value) }
        .asSequence()
        .let(block)

    override fun clear() {
        index.clear()
    }

}

class HashIndexFactory<K>: IndexFactory<K> {

    override fun create(indexName: String): Index<K> = HashIndex()
}
