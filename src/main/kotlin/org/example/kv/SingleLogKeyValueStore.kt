package org.example.kv

import org.example.DataEntry
import org.example.concepts.AppendMixin
import org.example.concepts.SizeMixin
import org.example.log.EntryLogFactory
import org.example.log.EntryWithOffset
import org.example.log.Log
import org.example.possiblyArrayEquals

private class SingleLogKeyValueStore<K, V>(
    override val log: Log<Map.Entry<K, V>>,
    private val tombstone: V
): LogKeyValueStore<K, V>,
    AppendMixin<Map.Entry<K, V>, Long> by log,
    SizeMixin by log {

    override fun get(key: K): V? = this[key, 0]

    override fun append(entry: Map.Entry<K, V>): Long {
        require(!possiblyArrayEquals(entry.value, tombstone)) {
            "Value $tombstone is reserved, and cannot be inserted."
        }
        return log.append(entry)
    }

    override fun delete(key: K) {
        log.append(DataEntry(key, tombstone))
    }

    override fun get(key: K, offset: Long?) = getWithTombstone(key, offset)
            ?.takeUnless { possiblyArrayEquals(it, tombstone) }

    override fun getWithOffset(key: K) = log.useEntriesWithOffset { it.findLastKey(key) }
            ?.takeUnless { possiblyArrayEquals(it.value, tombstone) }

    override fun getWithTombstone(key: K, offset: Long?): V? = log.useEntries(offset ?: 0L) { it.findLastKey(key) }

    private fun Sequence<Map.Entry<K, V>>.findLastKey(key: K): V? = this
            .findLast { possiblyArrayEquals(key, it.key) }?.value

    private fun Sequence<EntryWithOffset<Map.Entry<K, V>>>.findLastKey(key: K): ValueWithOffset<V>? = this
            .map { Pair(it.offset, it.entry) }
            .findLast { possiblyArrayEquals(key, it.second.key) }?.let { ValueWithOffset(it.first, it.second.value) }

    override fun contains(key: K) = log.useEntries {
        val lastValue = it.findLastKey(key)
        lastValue != null && !possiblyArrayEquals(lastValue, tombstone)
    }

    // TODO it's more efficient to just create a new log and trash the current one
    override fun clear() {
        val keysToBeDeleted = useEntries { entries -> entries.map { it.key }.toList() }
        keysToBeDeleted.forEach { delete(it) }
    }

}

internal class SingleLogKeyValueStoreFactory<K, V>(
    override val logFactory: EntryLogFactory<K, V>,
    private val tombstone: V
): LogKeyValueStoreFactory<K, V>, PropertyLogKeyValueStoreFactoryMixin<K, V> {

    override fun createFromPair(log: Log<Map.Entry<K, V>>): LogKeyValueStore<K, V>
            = SingleLogKeyValueStore(log, tombstone)
}
