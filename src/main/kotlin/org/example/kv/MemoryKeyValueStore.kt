package org.example.kv

import org.example.concepts.ImmutableDictionaryMixin
import org.example.concepts.MutableDictionaryMixin
import org.example.concepts.asImmutableDictionaryMixin
import org.example.concepts.asMutableDictionaryMixin

class MemoryKeyValueStore<K, V>(
    private val map: MutableMap<K, V> = mutableMapOf()
): KeyValueStore<K, V>,
    ImmutableDictionaryMixin<K, V> by map.asImmutableDictionaryMixin(),
    MutableDictionaryMixin<K, V> by map.asMutableDictionaryMixin()
