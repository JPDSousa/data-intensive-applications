package org.example.kv

import kotlinx.serialization.ExperimentalSerializationApi
import org.example.TestResources
import org.example.encoder.Encoders
import org.example.index.IndexFactories
import org.example.log.LogFactories
import org.junit.jupiter.api.AfterAll
import java.util.concurrent.atomic.AtomicLong


abstract class AbstractIndexedKeyValueStoreTest<K, V>: KeyValueStoreTest<K, V> {

    protected val uniqueGenerator = AtomicLong()

    companion object {

        @JvmStatic
        internal val resources = TestResources()

        @JvmStatic
        internal val encoders = Encoders()

        @JvmStatic
        internal val kvs = KeyValueStores(
                LogKeyValueStores(encoders, IndexFactories(resources)),
                LogFactories(encoders),
                resources
        )

        @JvmStatic
        @AfterAll
        fun closeResources() {
            resources.close()
        }

    }

}

internal class StringIndexedKeyValueStoreTest: AbstractIndexedKeyValueStoreTest<String, String>() {

    @ExperimentalSerializationApi
    override fun instances() = kvs.stringKeyValueStores()

    override fun nextKey() = uniqueGenerator.getAndIncrement()
            .toString()

    override fun nextValue() = uniqueGenerator.getAndIncrement()
            .toString()
}

internal class BinaryIndexedKeyValueStoreTest: AbstractIndexedKeyValueStoreTest<ByteArray, ByteArray>() {

    @ExperimentalSerializationApi
    override fun instances() = kvs.binaryKeyValueStores()

    override fun nextKey() = uniqueGenerator.getAndIncrement()
            .toString()
            .toByteArray()

    override fun nextValue() = uniqueGenerator.getAndIncrement()
            .toString()
            .toByteArray()

}
