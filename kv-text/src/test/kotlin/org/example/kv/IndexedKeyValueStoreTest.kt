package org.example.kv

import kotlinx.serialization.ExperimentalSerializationApi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.util.concurrent.atomic.AtomicLong


abstract class AbstractIndexedKeyValueStoreTest<K, V>: KeyValueStoreTest<K, V> {

    protected var kvs: KeyValueStores? = null
    protected val uniqueGenerator = AtomicLong()

    @BeforeEach fun openResources() {
        kvs = KeyValueStores()
    }

    @AfterEach fun closeResources() {
        kvs!!.close()
    }

}

internal class StringIndexedKeyValueStoreTest: AbstractIndexedKeyValueStoreTest<String, String>() {

    @ExperimentalSerializationApi
    override fun instances() = kvs!!.stringKeyValueStores()

    override fun nextKey() = uniqueGenerator.getAndIncrement()
            .toString()

    override fun nextValue() = uniqueGenerator.getAndIncrement()
            .toString()
}

internal class BinaryIndexedKeyValueStoreTest: AbstractIndexedKeyValueStoreTest<ByteArray, ByteArray>() {

    @ExperimentalSerializationApi
    override fun instances() = kvs!!.binaryKeyValueStores()

    override fun nextKey() = uniqueGenerator.getAndIncrement()
            .toString()
            .toByteArray()

    override fun nextValue() = uniqueGenerator.getAndIncrement()
            .toString()
            .toByteArray()

}
