package org.example.kv

import org.example.DataEntry
import org.example.log.EntryWithOffset
import org.example.log.Log
import org.example.possiblyArrayEquals

private class SingleLogKeyValueStore<K, V>(override val log: Log<Map.Entry<K, V>>,
                                            private val tombstone: V): LogBasedKeyValueStore<K, V> {

    override fun put(key: K, value: V) {
        append(key, value)
    }

    override fun get(key: K): V? = get(key, 0)

    override fun delete(key: K) {
        log.append(DataEntry(key, tombstone))
    }

    override fun clear() = log.clear()

    override fun append(key: K, value: V) = log.append(DataEntry(key, value))

    override fun appendAll(entries: Map<K, V>) = when {
        entries.isEmpty() -> emptySequence()
        else -> {
            val content = ArrayList<Map.Entry<K, V>>(entries.size)
            entries.forEach { (key, value) -> content.add(DataEntry(key, value)) }

            log.appendAll(content.asSequence())
        }
    }

    override fun get(key: K, offset: Long) = getWithTombstone(key)
            ?.takeUnless { possiblyArrayEquals(it, tombstone) }

    override fun getWithOffset(key: K) = log.useEntriesWithOffset { it.findLastKey(key) }
            ?.takeUnless { possiblyArrayEquals(it.value, tombstone) }

    override fun getWithTombstone(key: K): V? = log.useEntries(0) { it.findLastKey(key) }

    private fun Sequence<Map.Entry<K, V>>.findLastKey(key: K): V? = this
            .findLast { possiblyArrayEquals(key, it.key) }?.value

    private fun Sequence<EntryWithOffset<Map.Entry<K, V>>>.findLastKey(key: K): ValueWithOffset<V>? = this
            .map { Pair(it.offset, it.entry) }
            .findLast { possiblyArrayEquals(key, it.second.key) }?.let { ValueWithOffset(it.first, it.second.value) }


}

class SingleLogKeyValueStoreFactory<K, V>(private val tombstone: V): LogBasedKeyValueStoreFactory<K, V> {

    override fun createFromPair(log: Log<Map.Entry<K, V>>): LogBasedKeyValueStore<K, V>
            = SingleLogKeyValueStore(log, tombstone)
}
