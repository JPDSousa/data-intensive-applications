package org.example.kv

import org.example.encoder.Encoder
import org.example.log.EntryWithOffset
import org.example.log.Log
import org.example.log.LogEncoder
import org.example.possiblyArrayEquals

private class SingleLogKeyValueStore<K, V>(override val log: Log<Pair<K, V>>,
                                            private val tombstone: V): LogBasedKeyValueStore<K, V> {

    override fun put(key: K, value: V) {
        append(key, value)
    }

    override fun get(key: K): V? = get(key, 0)

    override fun delete(key: K) {
        log.append(Pair(key, tombstone))
    }

    override fun clear() = log.clear()

    override fun append(key: K, value: V) = log.append(Pair(key, value))

    override fun appendAll(entries: Map<K, V>) = when {
        entries.isEmpty() -> emptyList()
        else -> {
            val content = ArrayList<Pair<K, V>>(entries.size)
            entries.forEach { (key, value) -> content.add(Pair(key, value)) }

            log.appendAll(content)
        }
    }

    override fun get(key: K, offset: Long) = getWithTombstone(key)
            ?.takeUnless { possiblyArrayEquals(it, tombstone) }

    override fun getWithOffset(key: K) = log.useEntriesWithOffset { it.findLastKey(key) }
            ?.takeUnless { possiblyArrayEquals(it.value, tombstone) }

    override fun getWithTombstone(key: K): V? = log.useEntries(0) { it.findLastKey(key) }

    private fun Sequence<Pair<K, V>>.findLastKey(key: K): V? = this
            .findLast { possiblyArrayEquals(key, it.first) }?.second

    private fun Sequence<EntryWithOffset<Pair<K, V>>>.findLastKey(key: K): ValueWithOffset<V>? = this
            .map { Pair(it.offset, it.entry) }
            .findLast { possiblyArrayEquals(key, it.second.first) }?.let { ValueWithOffset(it.first, it.second.second) }

}

class SingleLogKeyValueStoreFactory<E, K, V>(private val tombstone: V,
                                             private val encoder: Encoder<Pair<K, V>, E>
): LogBasedKeyValueStoreFactory<E, K, V> {

    override fun create(log: Log<E>): LogBasedKeyValueStore<K, V>
            = SingleLogKeyValueStore(LogEncoder(log, encoder), tombstone)
}
