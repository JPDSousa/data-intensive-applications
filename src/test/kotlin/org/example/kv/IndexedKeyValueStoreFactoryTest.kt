package org.example.kv

import org.example.DataEntry
import org.example.TestInstance
import org.example.TestResources
import org.example.application
import org.example.generator.StringGenerator
import org.example.log.LogFactory
import org.example.log.StringStringMapEntryLogFactories
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.koin.core.KoinApplication

internal abstract class AbstractIndexedKeyValueStoreFactoryTest<K, V>: LogKeyValueStoreFactoryTest<K, V> {

    override val resources
        get() = application.koin.get<TestResources>()

    companion object {

        @JvmStatic
        internal lateinit var application: KoinApplication

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

    override fun logFactories(): Sequence<TestInstance<LogFactory<Map.Entry<String, String>>>> = logFactoriesGenerator
        .generate()

    companion object {

        private val logFactoriesGenerator: StringStringMapEntryLogFactories = application.koin.get()

        @JvmStatic
        private val factories: StringStringLogKeyValueStoreFactories = application.koin.get()

        @JvmStatic
        private val stringGenerator: StringGenerator = application.koin.get()

    }
}
