package org.example.kv

import org.example.log.Log

interface KeyValueStore<K, V> {

    fun put(key: K, value: V)

    fun putAll(entries: Map<K, V>) {
        entries.forEach(this::put)
    }

    fun get(key: K): V?

    fun delete(key: K)

    fun clear()
}

interface LogKeyValueStore<K, V>: TombstoneKeyValueStore<K, V> {

    fun append(key: K, value: V): Long

    fun appendAll(entries: Map<K, V>): Sequence<Long>

    fun getWithOffset(key: K): ValueWithOffset<V>?

    fun <R> useEntries(offset: Long = 0L, block: (Sequence<KeyValueEntry<K, V>>) -> R): R = log.useEntriesWithOffset(offset) {
        it.map { logEntry -> KeyValueEntry(logEntry.entry, logEntry.offset) }
            .let(block)
    }

    val log: Log<Map.Entry<K, V>>

    val size: Long
        get() = log.size

    val lastOffset: Long
        get() = log.lastOffset

}

interface TombstoneKeyValueStore<K, V>: KeyValueStore<K, V> {

    fun getWithTombstone(key: K, offset: Long? = null): V?

    fun get(key: K, offset: Long?): V?

    override fun get(key: K) = get(key, null)
}

data class ValueWithOffset<V>(val offset: Long, val value: V)

interface LogKeyValueStoreFactory<K, V> {

    fun createFromLog(log: Log<Map.Entry<K, V>>): LogKeyValueStore<K, V>
}
