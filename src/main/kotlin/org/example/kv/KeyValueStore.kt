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

interface LogBasedKeyValueStore<K, V>: KeyValueStore<K, V> {

    fun append(key: K, value: V): Long

    fun appendAll(entries: Map<K, V>): Collection<Long>

    fun get(key: K, offset: Long): V?

    fun getWithTombstone(key: K): V?

    fun getWithOffset(key: K): ValueWithOffset<V>?

    val log: Log<Pair<K, V>>

}

data class ValueWithOffset<V>(val offset: Long, val value: V)

interface LogBasedKeyValueStoreFactory<E, K, V> {

    fun create(log: Log<E>): LogBasedKeyValueStore<K, V>
}
