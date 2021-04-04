package org.example.kv

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.serialization.ExperimentalSerializationApi
import org.example.TestResources
import org.example.encoder.Encoders
import org.example.index.IndexFactories
import org.example.log.LogFactories
import org.example.size.ByteArraySizeCalculator
import org.example.size.StringSizeCalculator
import org.junit.jupiter.api.AfterAll
import java.nio.charset.Charset
import java.util.concurrent.Executors.newSingleThreadExecutor
import java.util.concurrent.atomic.AtomicLong


abstract class AbstractIndexedKeyValueStoreTest<K, V>: KeyValueStoreTest<K, V> {

    protected val uniqueGenerator = AtomicLong()

    companion object {

        @JvmStatic
        internal val resources = TestResources()

        @JvmStatic
        internal val encoders = Encoders()

        @JvmStatic
        internal val dispatcher= newSingleThreadExecutor().asCoroutineDispatcher()

        @JvmStatic
        internal val kvs = KeyValueStores(
            LogKeyValueStores(IndexFactories(resources, dispatcher)),
            LogFactories(encoders),
            encoders,
            ByteArraySizeCalculator,
            StringSizeCalculator(Charset.defaultCharset(), ByteArraySizeCalculator),
            resources,
            dispatcher
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
