package org.example.kv.lsm.sstable

import mu.KotlinLogging
import org.example.concepts.*
import org.example.kv.LogKeyValueStoreFactory
import org.example.kv.TombstoneKeyValueStore
import org.example.kv.lsm.SegmentDirectory
import org.example.size.SizeCalculator
import java.nio.file.Paths
import java.util.*

interface MemTable<K, V>
    : ImmutableDictionaryMixin<K, V>,
    MutableDictionaryMixin<K, V>,
    ClearMixin,
    Iterable<Map.Entry<K, V>>,
    SerializationMixin

private class MapMemTable<K: Comparable<K>, V>(
    private val memTable: SortedMap<K, V>,
    private val keySize: SizeCalculator<K>,
    private val valueSize: SizeCalculator<V>,
): MemTable<K, V>,
    ImmutableDictionaryMixin<K, V> by memTable.asImmutableDictionaryMixin(){

    override var byteLength: Long = 0L

    override fun clear() {
        memTable.clear()
        byteLength = 0L
    }

    override fun iterator() = memTable.iterator()

    override fun put(key: K, value: V) {
        memTable[key] = value
        byteLength += keySize.sizeOf(key)
        byteLength += valueSize.sizeOf(value)
    }

    override fun delete(key: K) {
        val oldValue = memTable.remove(key)
        byteLength -= keySize.sizeOf(key)
        if (oldValue != null) {
            byteLength -= valueSize.sizeOf(oldValue)
        }
    }

}

private class WriteAheadMemTable<K, V>(
    private val memTable: MemTable<K, V>,
    private val writeAhead: TombstoneKeyValueStore<K, V>,
): MemTable<K, V>,
    ImmutableDictionaryMixin<K, V> by memTable,
    SerializationMixin by memTable,
    Iterable<Map.Entry<K, V>> by memTable {

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
