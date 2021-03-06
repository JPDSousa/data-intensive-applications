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

interface LogBasedKeyValueStore<K, V>: TombstoneKeyValueStore<K, V> {

    fun append(key: K, value: V): Long

    fun appendAll(entries: Map<K, V>): Sequence<Long>

    fun get(key: K, offset: Long): V?

    fun getWithOffset(key: K): ValueWithOffset<V>?

    fun <R> useEntries(block: (Sequence<Map.Entry<K, V>>) -> R): R = log.useEntries(0) { block(it) }

    val log: Log<Map.Entry<K, V>>

    val size: Long
    get() = log.size

}

interface TombstoneKeyValueStore<K, V>: KeyValueStore<K, V> {

    fun getWithTombstone(key: K): V?

}

data class ValueWithOffset<V>(val offset: Long, val value: V)

interface LogBasedKeyValueStoreFactory<K, V> {

    fun createFromPair(log: Log<Map.Entry<K, V>>): LogBasedKeyValueStore<K, V>
}
