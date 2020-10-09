package org.example.kv

interface SeekableKeyValueStore: KeyValueStore {

    fun putAndGetOffset(key: String, value: String): Long

    fun get(key: String, offset: Long): String?

    fun getWithOffset(key: String): Pair<Long, String>?

}
