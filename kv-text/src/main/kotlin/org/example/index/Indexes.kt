package org.example.index

import kotlinx.serialization.ExperimentalSerializationApi
import org.example.TestInstance
import org.example.log.Index
import org.example.log.Logs
import java.io.Closeable

class Indexes: Closeable {

    private val factory = IndexFactory()
    private val logs = Logs()

    @ExperimentalSerializationApi
    fun <K> instances(): Sequence<TestInstance<Index<K>>> {
        val volatileTree = TestInstance<Index<K>>("Tree String Index", factory.createTreeIndex())

        return sequenceOf(volatileTree) + checkpointableIndexes()
    }

    @ExperimentalSerializationApi
    fun <K> checkpointableIndexes(): Sequence<TestInstance<Index<K>>> {

        val strIndexes = logs.stringInstances().map{
            TestInstance(it.name, factory.createStringLogEncoder<K>(it.instance))
        }.map {
            TestInstance("Checkpoint ~ ${it.name}", factory.createTreeIndex(it.instance))
        }

        val binaryIndexes = logs.binaryInstances().map {
            TestInstance(it.name, factory.createBinaryLogEncoder<K>(it.instance))
        }.map {
            TestInstance("Checkpoint ~ ${it.name}", factory.createTreeIndex(it.instance))
        }

        return strIndexes + binaryIndexes
    }

    override fun close() {
        logs.close()
    }
}
