package org.example.kv.lsm.sstable

import mu.KotlinLogging
import org.example.concepts.ClearMixin
import org.example.concepts.ImmutableDictionaryMixin
import org.example.concepts.MutableDictionaryMixin
import org.example.kv.LogKeyValueStoreFactory
import org.example.kv.TombstoneKeyValueStore
import org.example.kv.lsm.SegmentDirectory
import org.example.size.SizeCalculator
import java.nio.file.Paths
import java.util.*

interface MemTable<K, V>: ImmutableDictionaryMixin<K, V>, MutableDictionaryMixin<K, V>, ClearMixin,
    Iterable<Map.Entry<K, V>> {

    val byteSize: Long
}

private class MapMemTable<K: Comparable<K>, V>(
    private val memTable: SortedMap<K, V>,
    private val keySize: SizeCalculator<K>,
    private val valueSize: SizeCalculator<V>,
): MemTable<K, V> {

    override var byteSize: Long = 0L

    override fun clear() {
        memTable.clear()
        byteSize = 0L
    }

    override fun iterator() = memTable.iterator()

    override fun put(key: K, value: V) {
        memTable[key] = value
        byteSize += keySize.sizeOf(key)
        byteSize += valueSize.sizeOf(value)
    }

    override fun delete(key: K) {
        val oldValue = memTable.remove(key)
        byteSize -= keySize.sizeOf(key)
        if (oldValue != null) {
            byteSize -= valueSize.sizeOf(oldValue)
        }
    }

    override fun get(key: K): V? = memTable[key]

}

private class WriteAheadMemTable<K, V>(
    private val memTable: MemTable<K, V>,
    private val writeAhead: TombstoneKeyValueStore<K, V>,
): MemTable<K, V> {

    override val byteSize: Long
    get() = memTable.byteSize

    override fun put(key: K, value: V) {
        memTable[key] = value
        writeAhead[key] = value
    }

    override fun clear() {
        memTable.clear()
        writeAhead.clear()
    }

    override fun delete(key: K) {
        memTable.delete(key)
        writeAhead.delete(key)
    }

    override fun get(key: K): V? = memTable[key]

    override fun iterator() = memTable.iterator()
}

class MemTableFactory<K: Comparable<K>, V>(private val kvFactory: LogKeyValueStoreFactory<K, V>,
                                           private val keySize: SizeCalculator<K>,
                                           private val valueSize: SizeCalculator<V>) {

    private val logger = KotlinLogging.logger {}

    fun createMemTable(segmentDirectory: SegmentDirectory, segmentId: Int): MemTable<K, V> {
        val writeAheadKV = kvFactory.createFromPair(segmentDirectory.createMemTableFile(segmentId))
        val mapMemTable = MapMemTable(
            TreeMap(),
            keySize,
            valueSize
        )

        writeAheadKV.useEntries {
            logger.debug { "Recovering mem table" }
            it.forEach { (key, offset) -> mapMemTable[key] = offset }
        }

        return WriteAheadMemTable(
            mapMemTable,
            writeAheadKV
        )
    }
}

private fun SegmentDirectory.createMemTableFile(segmentId: Int) = "memtable-${segmentId}"
    .let { Paths.get(it) }
    .let { createFile(it, allowExisting = true) }
