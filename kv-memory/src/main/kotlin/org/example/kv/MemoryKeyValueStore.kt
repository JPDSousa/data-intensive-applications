package org.example.kv

class MemoryKeyValueStore(private val map: MutableMap<String, String> = mutableMapOf()): KeyValueStore {

    override fun put(key: String, value: String) { map[key] = value }

    override fun putAll(entries: Map<String, String>) { map.putAll(entries) }

    override fun get(key: String): String? = map[key]

    override fun delete(key: String) { map.remove(key) }

    override fun clear() { map.clear() }
}
