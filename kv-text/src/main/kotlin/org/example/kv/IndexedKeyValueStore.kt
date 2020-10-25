package org.example.kv

import org.example.log.EntryWithOffset
import org.example.log.Index
import org.example.log.IndexEntry
import org.example.log.Log

internal class IndexedKeyValueStore<E, K, V>(private val index: Index<K>,
                                             internal val log: Log<E>,
                                             private val encoder: EntryEncoder<E, K, V>,
                                             private val tombstone: V): KeyValueStore<K, V> {

    override fun put(key: K, value: V) {

        val offset = putAndGetOffset(key, value)
        this.index.putOffset(key, offset)
    }

    override fun putAll(entries: Map<K, V>) {

        if (entries.isNotEmpty()) {
            val content = ArrayList<E>(entries.size)
            val keys = ArrayList<K>(entries.size)

            for (entry in entries) {
                content.add(encoder.encode(entry.key, entry.value))
                keys.add(entry.key)
            }

            val offsets = log.appendAll(content)

            index.putAllOffsets(keys.zipIndex(offsets))
        }
    }

    private fun putAndGetOffset(key: K, value: V) = log.append(encoder.encode(key, value))

    private fun get(key: K, offset: Long): V? = log.useEntries(offset) { it.findLastKey(key) }

    private fun getRaw(key: K, nullifyTombstone: Boolean): V? {

        val offset = index.getOffset(key)

        if (offset == tombstoneIndex) {
            return if (nullifyTombstone) null else tombstone
        }

        if (offset != null) {
            return get(key, offset)
        }

        return getWithOffset(key)
                ?.also { index.putOffset(key, it.first) }
                ?.second
    }

    internal fun getWithTombstone(key: K) = getRaw(key, false)

    override fun get(key: K) = getRaw(key, true)

    override fun delete(key: K) {
        log.append(encoder.encode(key, tombstone))
        index.putOffset(key, tombstoneIndex)
    }

    override fun clear() = log.clear()

    internal fun <T> forEachEntry(block: (Pair<K, V>) -> T) = log.useEntries {
        it.forEach { entry -> block(encoder.decode(entry)) }
    }

    private fun getWithOffset(key: K): Pair<Long, V>? = log.useEntriesWithOffset { it.findLastKey(key) }

    private fun Sequence<E>.findLastKey(key: K): V? = this
            .map { encoder.decode(it) }
            .findLast { keyEquals(key, it.first) }?.second

    private fun Sequence<EntryWithOffset<E>>.findLastKey(key: K) : Pair<Long, V>? = this
            .map { Pair(it.offset, encoder.decode(it.entry)) }
            .findLast { keyEquals(key, it.second.first) }?.let { Pair(it.first, it.second.second) }

    private fun ArrayList<K>.zipIndex(other: Collection<Long>): Iterable<IndexEntry<K>> {

        val indexes = ArrayList<IndexEntry<K>>(size)
        val thisIterator = iterator()
        val thatIterator = other.iterator()

        while(thisIterator.hasNext() && thatIterator.hasNext()) {
            indexes.add(IndexEntry(thisIterator.next(), thatIterator.next()))
        }

        return indexes
    }

    companion object {
        private const val tombstoneIndex = -1L
    }
}

private fun keyEquals(val1: Any?, val2: Any?): Boolean {

    if (val1 is ByteArray && val2 is ByteArray) {
        return val1.contentEquals(val2)
    }

    return val1 == val2
}
