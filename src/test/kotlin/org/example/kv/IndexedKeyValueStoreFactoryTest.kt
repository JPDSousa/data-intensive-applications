package org.example.kv

import org.example.ApplicationTest
import org.example.DataEntry
import org.example.TestInstance
import org.example.TestResources
import org.example.generator.StringGenerator
import org.example.log.LogFactory
import org.example.log.StringStringMapEntryLogFactories

internal abstract class AbstractIndexedKeyValueStoreFactoryTest<K, V>: ApplicationTest(), LogKeyValueStoreFactoryTest<K, V> {

    override val resources
        get() = application.koin.get<TestResources>()
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
