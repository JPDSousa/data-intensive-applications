package org.example.kv

interface KeyValueStore {

    fun put(key: String, value: String)

    fun putAll(entries: Map<String, String>) {
        entries.forEach(this::put)
    }

    fun get(key: String): String?

    fun delete(key: String)
}
