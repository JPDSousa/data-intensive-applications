package org.example.kv

import org.example.index.Index
import org.example.index.IndexEntry
import org.example.index.IndexFactory
import org.example.log.Log
import java.util.concurrent.atomic.AtomicLong

private class IndexedKeyValueStore<K, V>(
        private val index: Index<K>,
        private val tombstone: V,
        private val logKV: LogBasedKeyValueStore<K, V>): LogBasedKeyValueStore<K, V> by logKV {

    override fun put(key: K, value: V) {

        val offset = logKV.append(key, value)
        this.index.putOffset(key, offset)
    }

    override fun putAll(entries: Map<K, V>) {

        if (entries.isNotEmpty()) {
            val offsets = logKV.appendAll(entries)

            index.putAllOffsets(entries.keys.zipIndex(offsets))
        }
    }

    private fun getRaw(key: K, nullifyTombstone: Boolean): V? {

        val offset = index.getOffset(key)

        if (offset == tombstoneIndex) {
            return if (nullifyTombstone) null else tombstone
        }

        if (offset != null) {
            return logKV.get(key, offset)
        }

        return logKV.getWithOffset(key)
                ?.also { index.putOffset(key, it.offset) }
                ?.value
    }

    override fun getWithTombstone(key: K) = getRaw(key, false)

    override fun get(key: K) = getRaw(key, true)

    override fun delete(key: K) {
        logKV.delete(key)
        index.putOffset(key, tombstoneIndex)
    }

    override fun clear() = logKV.clear()

    private fun Set<K>.zipIndex(other: Sequence<Long>): Iterable<IndexEntry<K>> {

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

class IndexedKeyValueStoreFactory<K, V>(private val indexFactory: IndexFactory<K>,
                                        private val tombstone: V,
                                        private val innerKVSFactory: LogBasedKeyValueStoreFactory<K, V>,
                                        private val nameGenerator: AtomicLong = AtomicLong(0))
    : LogBasedKeyValueStoreFactory<K, V> {

    override fun createFromPair(log: Log<Map.Entry<K, V>>): LogBasedKeyValueStore<K, V> = IndexedKeyValueStore(
        indexFactory.create("Index${nameGenerator.getAndIncrement()}"),
        tombstone,
        innerKVSFactory.createFromPair(log)
    )
}
