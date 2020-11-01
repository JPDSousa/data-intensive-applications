package org.example.index

import kotlinx.serialization.ExperimentalSerializationApi
import org.example.TestInstance
import org.example.log.Index
import org.example.log.IndexEntry
import org.example.log.Log
import org.example.log.Logs
import java.io.Closeable

class Indexes: Closeable {

    private val factory = IndexFactory()
    private val logs = Logs()

    @ExperimentalSerializationApi
    fun <K : Comparable<K>> instances(): Sequence<TestInstance<Index<K>>> = comparableInstances<K>() +
            nonComparableInstances()

    @ExperimentalSerializationApi
    fun <K> nonComparableInstances(): Sequence<TestInstance<Index<K>>> {

        val volatileHash = TestInstance<Index<K>>("Hash Index", factory.createHashIndex())

        return sequenceOf(volatileHash) + checkpointableNonComparableIndexes()
    }

    @ExperimentalSerializationApi
    private fun <K : Comparable<K>> comparableInstances(): Sequence<TestInstance<Index<K>>> {
        val volatileTree = TestInstance<Index<K>>("Tree Index", factory.createTreeIndex())

        return sequenceOf(volatileTree) + checkpointableComparableIndexes()
    }

    @ExperimentalSerializationApi
    private fun <K: Comparable<K>> checkpointableComparableIndexes(): Sequence<TestInstance<Index<K>>>
            = checkpointableIndexes { factory.createTreeIndex(it) }

    @ExperimentalSerializationApi
    fun <K> checkpointableNonComparableIndexes(): Sequence<TestInstance<Index<K>>>
            = checkpointableIndexes { factory.createHashIndex(it) }

    @ExperimentalSerializationApi
    private fun <K> checkpointableIndexes(creator: (Log<IndexEntry<K>>) -> Index<K>): Sequence<TestInstance<Index<K>>> {

        val strIndexes = logs.stringInstances().map{
            TestInstance(it.name, factory.createStringLogEncoder<K>(it.instance))
        }.map {
            TestInstance("Checkpoint ~ ${it.name}", creator(it.instance))
        }

        val binaryIndexes = logs.binaryInstances().map {
            TestInstance(it.name, factory.createBinaryLogEncoder<K>(it.instance))
        }.map {
            TestInstance("Checkpoint ~ ${it.name}", creator(it.instance))
        }

        return strIndexes + binaryIndexes
    }

    override fun close() {
        logs.close()
    }
}
