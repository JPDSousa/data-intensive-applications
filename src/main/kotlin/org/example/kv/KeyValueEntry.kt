package org.example.kv

data class KeyValueEntry<K, V>(override val key: K, override val value: V, val offset: Long)
    : Map.Entry<K, V> {

        constructor(entry: Map.Entry<K, V>, offset: Long)
                : this(entry.key, entry.value, offset)
    }

fun <K, V> Sequence<KeyValueEntry<K, V>>.toMap() = associate { Pair(it.key, it.value) }