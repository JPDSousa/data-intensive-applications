package org.example.kv

import org.example.DataEntry
import org.example.generator.Generator
import org.example.index.CheckpointableIndexFactory
import org.example.index.Index
import org.example.index.IndexEntry
import org.example.log.Log
import org.example.log.LogFactory

private class IndexedKeyValueStore<K, V>(
        private val index: Index<K>,
        private val tombstone: V,
        private val logKV: LogKeyValueStore<K, V>): LogKeyValueStore<K, V> by logKV {

    override fun contains(key: K) = key in index && index[key] != tombstoneIndex

    override fun put(key: K, value: V) {

        val offset = append(DataEntry(key, value))
        this.index[key] = offset
    }

    override fun putAll(entries: Map<out K, V>) {

        val offsets = logKV.appendAll(entries)
        if (entries.isNotEmpty()) {
            index.putAllOffsets(entries.keys.zipIndex(offsets))
        }
    }

    private fun getRaw(key: K, offset: Long?, nullifyTombstone: Boolean): V? {

        if (offset == tombstoneIndex) {
            return if (nullifyTombstone) null else tombstone
        }

        if (offset != null) {
            return logKV[key, offset]
        }

        return logKV.getWithOffset(key)
                ?.also { index[key] = it.offset }
                ?.value
    }

    override fun getWithTombstone(key: K, offset: Long?) = getRaw(
        key,
        offset ?: index[key],
        false
    )

    override fun get(key: K, offset: Long?) = getRaw(
        key,
        offset ?: index[key],
        true
    )

    override fun delete(key: K) {
        logKV.delete(key)
        index[key] = tombstoneIndex
    }

    override fun clear() {
        index.clear()
        logKV.clear()
    }

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

class IndexedKeyValueStoreFactory<K, V>(private val indexFactory: CheckpointableIndexFactory<K>,
                                        private val tombstone: V,
                                        private val innerKVSFactory: LogKeyValueStoreFactory<K, V>,
                                        override val logFactory: LogFactory<Map.Entry<K, V>>,
                                        nameGenerator: Generator<String>)
    : LogKeyValueStoreFactory<K, V>, PropertyLogKeyValueStoreFactoryMixin<K, V> {

    private val nameIterator = nameGenerator.generate().iterator()

    private fun generateName() = when {
        nameIterator.hasNext() -> nameIterator.next()
        else -> throw NoSuchElementException("Name generator cannot generate more names")
    }

    override fun createFromPair(log: Log<Map.Entry<K, V>>): LogKeyValueStore<K, V> {

        val index = indexFactory.create("Index${generateName()}")
        val logKV = innerKVSFactory.createFromPair(log)

        val indexLastOffset = index.lastOffset
        val kvLastOffset = logKV.lastOffset

        if (indexLastOffset > kvLastOffset) {
            // TODO is there a more efficient approach than rewritting the entire index
            index.clear()
            logKV.useEntries {
                it.forEach { entry -> index[entry.key] = entry.offset }
            }
        } else if (indexLastOffset < kvLastOffset) {
            // seek the last index offset in the log and update the index from there
            logKV.useEntries(indexLastOffset) {
                it.forEach { entry -> index[entry.key] = entry.offset }
            }
        }

        return IndexedKeyValueStore(
            index,
            tombstone,
            logKV
        )
    }
}
