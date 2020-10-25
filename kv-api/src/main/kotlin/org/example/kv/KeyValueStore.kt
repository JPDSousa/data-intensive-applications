package org.example.kv

interface KeyValueStore<K, V> {

    fun put(key: K, value: V)

    fun putAll(entries: Map<K, V>) {
        entries.forEach(this::put)
    }

    fun get(key: K): V?

    fun delete(key: K)

    fun clear()
}
