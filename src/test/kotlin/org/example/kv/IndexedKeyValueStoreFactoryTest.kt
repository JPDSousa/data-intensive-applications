package org.example.kv

import org.example.DataEntry
import org.example.TestResources
import org.example.application
import org.example.generator.StringGenerator
import org.example.log.LogFactory
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll

internal abstract class AbstractIndexedKeyValueStoreFactoryTest<K, V>: LogKeyValueStoreFactoryTest<K, V> {

    override val resources = application.koin.get<TestResources>()

    companion object {

        @JvmStatic
        internal var application = application()

        @JvmStatic
        @BeforeAll
        fun createApplication() {
            application = application()
        }

        @JvmStatic
        @AfterAll
        fun closeResources() {
            application.close()
        }

    }
}

internal class StringIndexedKeyValueStoreFactoryTest: AbstractIndexedKeyValueStoreFactoryTest<String, String>() {

    private val valueGenerator = stringGenerator.generate().iterator()

    override fun instances() = factories.generate()

    // TODO check for hasNext
    override fun nextEntry() = DataEntry(valueGenerator.next(), valueGenerator.next())

    override val logFactory: LogFactory<Map.Entry<String, String>>
        get() = TODO("Not yet implemented")

    companion object {

        @JvmStatic
        private val factories: StringStringLogKeyValueStoreFactories = application.koin.get()

        @JvmStatic
        private val stringGenerator: StringGenerator = AbstractIndexedKeyValueStoreTest.application.koin.get()

    }
}
