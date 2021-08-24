package org.example.kv

import org.example.TestGenerator
import org.example.TestGeneratorAdapter
import org.example.TestInstance
import org.example.generator.CompositeGenerator
import org.example.generator.Generator
import org.example.generator.StringGenerator
import org.example.index.CheckpointableIndexFactories
import org.example.index.LongCheckpointableIndexFactories
import org.example.index.StringCheckpointableIndexFactories
import org.koin.core.scope.Scope
import org.koin.dsl.module

val logKeyValueStoreFactoryModule = module {

    single<StringStringLogKeyValueStoreFactories> {
        DelegateStringStringLogKeyValueStoreFactories(
            composite<String, String, StringCheckpointableIndexFactories>(this, Tombstone.string))
    }

    single<LongByteArrayLogKeyValueStoreFactories> {
        DelegateLongByteArrayLogKeyValueStoreFactories(
            composite<Long, ByteArray, LongCheckpointableIndexFactories>(this, Tombstone.byte))
    }
}

private inline fun <K, V, reified C: CheckpointableIndexFactories<K>> composite(scope: Scope, tombstone: V)
        : TestGenerator<LogKeyValueStoreFactory<K, V>> {

    val singleFactories = SingleLogKeyValueStoreFactories<K, V>(tombstone)
    return TestGeneratorAdapter(
        CompositeGenerator(listOf(
            IndexedLogKeyValueStoreFactories(
                tombstone,
                scope.get<C>(),
                singleFactories,
                scope.get<StringGenerator>()
            ),
            singleFactories
        ))
    )
}

interface LogKeyValueStoreFactories<K, V>: TestGenerator<LogKeyValueStoreFactory<K, V>>
interface StringStringLogKeyValueStoreFactories: LogKeyValueStoreFactories<String, String>

interface LongByteArrayLogKeyValueStoreFactories: LogKeyValueStoreFactories<Long, ByteArray>

private class DelegateStringStringLogKeyValueStoreFactories(
    private val delegate: TestGenerator<LogKeyValueStoreFactory<String, String>>)
    : StringStringLogKeyValueStoreFactories, TestGenerator<LogKeyValueStoreFactory<String, String>> by delegate

private class DelegateLongByteArrayLogKeyValueStoreFactories(
    private val delegate: TestGenerator<LogKeyValueStoreFactory<Long, ByteArray>>)
    : LongByteArrayLogKeyValueStoreFactories, TestGenerator<LogKeyValueStoreFactory<Long, ByteArray>> by delegate

private class IndexedLogKeyValueStoreFactories<K, V>(private val tombstone: V,
                                                     private val indexFactories: CheckpointableIndexFactories<K>,
                                                     private val localKVSFactories: LogKeyValueStoreFactories<K, V>,
                                                     private val stringGenerator: Generator<String>)
    : LogKeyValueStoreFactories<K, V> {

    override fun generate(): Sequence<TestInstance<LogKeyValueStoreFactory<K, V>>> = sequence {

        for (indexFactory in indexFactories.generate()) {

            for (localKVSFactory in localKVSFactories.generate()) {

                yield(TestInstance("${IndexedKeyValueStoreFactory::class.simpleName} with ${indexFactory.name}, " +
                        "Tombstone '$tombstone', ${localKVSFactory.name} and $stringGenerator") {
                    IndexedKeyValueStoreFactory(
                        indexFactory.instance(),
                        tombstone,
                        localKVSFactory.instance(),
                        stringGenerator
                    )
                })
            }
        }
    }
}

private class SingleLogKeyValueStoreFactories<K, V>(private val tombstone: V): LogKeyValueStoreFactories<K, V> {

    override fun generate(): Sequence<TestInstance<LogKeyValueStoreFactory<K, V>>> = sequenceOf(
        TestInstance("${SingleLogKeyValueStoreFactory::class.simpleName} with Tombstone '$tombstone'") {
            SingleLogKeyValueStoreFactory(tombstone)
        }
    )
}
