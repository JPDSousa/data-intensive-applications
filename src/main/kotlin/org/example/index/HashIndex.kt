package org.example.index

private class HashIndex<K>(private val index: MutableMap<K, Long> = HashMap()): Index<K> {

    override fun putOffset(key: K, offset: Long) {
        index[key] = offset
    }

    override fun getOffset(key: K) = index[key]

    override fun putAllOffsets(pairs: Iterable<IndexEntry<K>>) {
        pairs.forEach { index[it.key] = it.offset }
    }

    override fun entries() = index.map { IndexEntry(it.key, it.value) }.asSequence()

}

class HashIndexFactory<K>: IndexFactory<K> {

    override fun create(indexName: String): Index<K> = HashIndex()
}
