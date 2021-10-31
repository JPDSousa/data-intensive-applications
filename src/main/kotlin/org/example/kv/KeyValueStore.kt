package org.example.kv

import org.example.DataEntry
import org.example.concepts.*
import org.example.log.Log
import org.example.log.LogFactory
import java.nio.file.Path

interface KeyValueStore<K, V>: ImmutableDictionaryMixin<K, V>, MutableDictionaryMixin<K, V>, ClearMixin

interface SerializableKeyValueStore<K, V>: KeyValueStore<K, V>, SerializationMixin

interface LogKeyValueStore<K, V>
    : TombstoneKeyValueStore<K, V>, AppendMixin<Map.Entry<K, V>, Long>, SerializationMixin {

    override fun put(key: K, value: V) {
        append(DataEntry(key, value))
    }

    fun appendAll(entries: Map<out K, V>): Sequence<Long> = when {
        entries.isEmpty() -> emptySequence()
        else -> {
            val content = ArrayList<Map.Entry<K, V>>(entries.size)
            entries.forEach { (key, value) -> content.add(DataEntry(key, value)) }

            appendAll(content.asSequence())
        }
    }

    fun getWithOffset(key: K): ValueWithOffset<V>?

    fun loadToMemory(): Map<K, V> = useEntries { entries -> entries.toMap() }

    fun <R> useEntries(offset: Long = 0L, block: (Sequence<KeyValueEntry<K, V>>) -> R): R = log.useEntriesWithOffset(offset) {
        it.map { logEntry -> KeyValueEntry(logEntry.entry, logEntry.offset) }
            .let(block)
    }

    val log: Log<Map.Entry<K, V>>

    override val byteLength: Long
        get() = log.byteLength

    val lastOffset: Long
        get() = log.lastOffset

}

interface TombstoneKeyValueStore<K, V>: KeyValueStore<K, V> {

    fun getWithTombstone(key: K, offset: Long? = null): V?

    // TODO Offset is a Log concept, which should not be exposed here
    operator fun get(key: K, offset: Long?): V?

    override fun get(key: K) = this[key, null]
}

data class ValueWithOffset<V>(val offset: Long, val value: V)

interface LogKeyValueStoreFactory<K, V> {

    fun createFromPair(log: Log<Map.Entry<K, V>>): LogKeyValueStore<K, V>

    fun createFromPair(logPath: Path): LogKeyValueStore<K, V>
}

interface PropertyLogKeyValueStoreFactoryMixin<K, V>: LogKeyValueStoreFactory<K, V> {

    val logFactory: LogFactory<Map.Entry<K, V>>

    override fun createFromPair(logPath: Path) = createFromPair(logPath.asLog())

    private fun Path.asLog() = logFactory.create(this)
}
