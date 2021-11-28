package org.example.kv

import org.example.concepts.*

class MemoryKeyValueStore<K, V>(
    private val map: MutableMap<K, V> = mutableMapOf()
): KeyValueStore<K, V>,
    ClearMixin by map.asClearMixin(),
    ImmutableDictionaryMixin<K, V> by map.asImmutableDictionaryMixin(),
    MutableDictionaryMixin<K, V> by map.asMutableDictionaryMixin()
