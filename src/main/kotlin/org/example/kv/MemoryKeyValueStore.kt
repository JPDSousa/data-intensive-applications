package org.example.kv

class MemoryKeyValueStore<K, V>(private val map: MutableMap<K, V> = mutableMapOf()): KeyValueStore<K, V> {

    override fun put(key: K, value: V) { map[key] = value }

    override fun putAll(entries: Map<out K, V>) { map.putAll(entries) }

    override fun get(key: K): V? = map[key]

    override fun delete(key: K) { map.remove(key) }

    override fun clear() { map.clear() }

}
