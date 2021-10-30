package org.example.kv

import kotlinx.serialization.ExperimentalSerializationApi
import org.example.ApplicationTest
import org.example.generator.ByteArrayGenerator
import org.example.generator.LongGenerator
import org.example.generator.StringGenerator


internal class StringIndexedKeyValueStoreTest: ApplicationTest(), KeyValueStoreTest<String, String> {

    private val valueGenerator = stringGenerator.generate().iterator()

    @ExperimentalSerializationApi
    override fun instances() = kvs.generate()

    override fun nextKey() = when {
        valueGenerator.hasNext() -> valueGenerator.next()
        else -> throw NoSuchElementException("No more values!")
    }

    override fun nextValue() = nextKey()

    companion object {

        @JvmStatic
        private val kvs: StringStringKeyValueStores = application.koin.get()

        @JvmStatic
        private val stringGenerator: StringGenerator = application.koin.get()
    }
}

internal class BinaryIndexedKeyValueStoreTest: ApplicationTest(), KeyValueStoreTest<Long, ByteArray> {

    private val keyGenerator = longGenerator.generate().iterator()

    private val valueGenerator = byteGenerator.generate().iterator()

    @ExperimentalSerializationApi
    override fun instances() = kvs.generate()

    override fun nextKey() = when {
        keyGenerator.hasNext() -> keyGenerator.next()
        else -> throw NoSuchElementException("No more values!")
    }

    override fun nextValue() = when {
        valueGenerator.hasNext() -> valueGenerator.next()
        else -> throw NoSuchElementException("No more values!")
    }

    companion object {

        @JvmStatic
        private val kvs: ByteArrayKeyValueStores = application.koin.get()

        @JvmStatic
        private val byteGenerator: ByteArrayGenerator = application.koin.get()

        @JvmStatic
        private val longGenerator: LongGenerator = application.koin.get()
    }

}
