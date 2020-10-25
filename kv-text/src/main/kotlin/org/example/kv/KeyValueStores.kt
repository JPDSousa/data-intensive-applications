package org.example.kv

import kotlinx.serialization.ExperimentalSerializationApi
import org.example.TestInstance
import org.example.index.Indexes
import org.example.log.Logs
import java.io.Closeable
import java.nio.file.Files
import java.nio.file.Files.delete
import java.nio.file.Path
import java.util.*

class KeyValueStores: Closeable {

    private val factory = KeyValueStoreFactory()
    private val indexes = Indexes()
    private val logs = Logs()
    private var resources = Stack<Path>()

    @ExperimentalSerializationApi
    fun binaryKeyValueStores(): Sequence<TestInstance<KeyValueStore<ByteArray, ByteArray>>> {

        val indexes = indexes.instances<ByteArray>().toList()
        val stringLogs = logs.stringInstances().toList()
        val binaryLogs = logs.binaryInstances().toList()

        return sequence {
            for (index in indexes) {

                for (stringLog in stringLogs) {
                    val name = "Indexed Key Value Store ~ ${index.name} ~ ${stringLog.name}"
                    yield(TestInstance(name, factory.createBinaryKeyValueStore(
                            index.instance,
                            stringLog.instance
                    )))
                }

                for (binaryLog in binaryLogs) {
                    val name = "Indexed Key Value Store ~ ${index.name} ~ ${binaryLog.name}"
                    yield(TestInstance(name, factory.createBinaryKeyValueStore(
                            index.instance,
                            binaryLog.instance
                    )))
                }
            }
            yieldAll(binarySegmentedKeyValueStores())
        }
    }

    fun binarySegmentedKeyValueStores(): Sequence<TestInstance<KeyValueStore<ByteArray, ByteArray>>> {

        val kvDir = Files.createTempDirectory("segmented-")
        resources.push(kvDir)

        return sequenceOf(TestInstance("Binary Segmented KV", factory.createSegmentedBinaryKeyValueStore(kvDir)))
    }

    @ExperimentalSerializationApi
    fun stringKeyValueStores(): Sequence<TestInstance<KeyValueStore<String, String>>> {

        val indexes = indexes.instances<String>().toList()
        val stringLogs = logs.stringInstances().toList()
        val binaryLogs = logs.binaryInstances().toList()

        return sequence {
            for (index in indexes) {

                for (stringLog in stringLogs) {
                    val name = "Indexed Key Value Store ~ ${index.name} ~ ${stringLog.name}"
                    yield(TestInstance(name, factory.createStringKeyValueStore(
                            index.instance,
                            stringLog.instance
                    )))
                }

                for (binaryLog in binaryLogs) {
                    val name = "Indexed Key Value Store ~ ${index.name} ~ ${binaryLog.name}"
                    yield(TestInstance(name, factory.createStringKeyValueStore(
                            index.instance,
                            binaryLog.instance
                    )))
                }
            }

            yieldAll(stringSegmentedKeyValueStores())
        }
    }

    fun stringSegmentedKeyValueStores(): Sequence<TestInstance<KeyValueStore<String, String>>> {
        val kvDir = Files.createTempDirectory("segmented-")
        resources.push(kvDir)

        return sequenceOf(TestInstance("String Segmented KV", factory.createSegmentedStringKeyValueStore(kvDir)))
    }

    override fun close() {

        logs.close()
        indexes.close()

        while (resources.isNotEmpty()) {
            resources.pop()?.let { delete(it) }
        }
    }

}
