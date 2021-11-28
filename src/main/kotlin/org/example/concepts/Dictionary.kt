package org.example.concepts

interface MutableDictionaryMixin<K, V> {

    operator fun set(key: K, value: V) = put(key, value)

    fun put(key: K, value: V)

    fun putAll(entries: Map<out K, V>) {
        entries.forEach(this::put)
    }

    fun delete(key: K)

}

fun <K, V> MutableMap<K, V>.asMutableDictionaryMixin(): MutableDictionaryMixin<K, V> {

    val map = this

    return object: MutableDictionaryMixin<K, V> {

        override fun put(key: K, value: V) {
            map[key] = value
        }

        override fun delete(key: K) {
            map.remove(key)
        }

    }
}

interface ImmutableDictionaryMixin<K, V> {

    operator fun get(key: K): V?

    operator fun contains(key: K): Boolean
}

fun <K, V> Map<K, V>.asImmutableDictionaryMixin(): ImmutableDictionaryMixin<K, V> {

    val map = this

    return object: ImmutableDictionaryMixin<K, V> {
        override fun get(key: K): V? = map[key]

        override fun contains(key: K) = key in map
    }
}