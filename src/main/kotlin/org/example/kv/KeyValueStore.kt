package org.example.kv

import org.example.log.Log

interface KeyValueStore<K, V> {

    fun put(key: K, value: V)

    fun putAll(entries: Map<K, V>) {
        entries.forEach(this::put)
    }

    // TODO feature leak here. Tombstones are implementation details specific to SOME types of KVs
    fun getWithTombstone(key: K): V?

    fun get(key: K): V?

    fun delete(key: K)

    fun clear()
}

interface LogBasedKeyValueStore<K, V>: KeyValueStore<K, V> {

    fun append(key: K, value: V): Long

    fun appendAll(entries: Map<K, V>): Sequence<Long>

    fun get(key: K, offset: Long): V?

    fun getWithOffset(key: K): ValueWithOffset<V>?

    fun <R> useEntries(block: (Sequence<Map.Entry<K, V>>) -> R): R = log.useEntries(0) { block(it) }

    val log: Log<Map.Entry<K, V>>

}

data class ValueWithOffset<V>(val offset: Long, val value: V)

interface LogBasedKeyValueStoreFactory<K, V> {

    fun createFromPair(log: Log<Map.Entry<K, V>>): LogBasedKeyValueStore<K, V>
}
