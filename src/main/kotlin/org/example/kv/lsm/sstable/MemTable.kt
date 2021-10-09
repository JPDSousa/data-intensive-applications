package org.example.kv.lsm.sstable

import org.example.kv.LogKeyValueStoreFactory
import org.example.kv.TombstoneKeyValueStore
import org.example.kv.lsm.SegmentDirectory
import org.example.size.SizeCalculator
import java.nio.file.Paths
import java.util.*

interface MemTable<K, V>{

    val byteSize: Long

    fun put(key: K, value: V)

    fun get(key: K): V?

    fun delete(key: K)

    fun clear()

    fun forEach(action: (K, V) -> Unit)
}

class WriteAheadMemTable<K: Comparable<K>, V>(private val memTable: SortedMap<K, V>,
                                              private val keySize: SizeCalculator<K>,
                                              private val valueSize: SizeCalculator<V>,
                                              private val writeAhead: TombstoneKeyValueStore<K, V>
): MemTable<K, V> {

    override var byteSize: Long = 0L

    override fun put(key: K, value: V) {
        memTable[key] = value
        byteSize += keySize.sizeOf(key)
        byteSize += valueSize.sizeOf(value)
        writeAhead.put(key, value)
    }

    override fun clear() {
        memTable.clear()
        byteSize = 0L
        writeAhead.clear()
    }

    override fun delete(key: K) {
        val oldValue = memTable.remove(key)
        byteSize -= keySize.sizeOf(key)
        if (oldValue != null) {
            byteSize -= valueSize.sizeOf(oldValue)
        }
        writeAhead.delete(key)
    }

    override fun get(key: K): V? = memTable[key]

    override fun forEach(action: (K, V) -> Unit) = memTable.forEach(action)
}

class MemTableFactory<K: Comparable<K>, V>(private val kvFactory: LogKeyValueStoreFactory<K, V>,
                                           private val keySize: SizeCalculator<K>,
                                           private val valueSize: SizeCalculator<V>) {

    fun createMemTable(segmentDirectory: SegmentDirectory, segmentId: Int): MemTable<K, V> = WriteAheadMemTable(
        TreeMap(),
        keySize,
        valueSize,
        kvFactory.createFromPair(segmentDirectory.createMemTableFile(segmentId))
    )
}

private fun SegmentDirectory.createMemTableFile(segmentId: Int) = "memtable-${segmentId}"
    .let { Paths.get(it) }
    .let { createFile(it) }
