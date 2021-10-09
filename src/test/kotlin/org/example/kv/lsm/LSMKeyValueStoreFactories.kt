package org.example.kv.lsm

import kotlinx.coroutines.CoroutineDispatcher
import org.example.TestGenerator
import org.example.TestInstance
import org.example.kv.Tombstone
import org.koin.dsl.module

interface LSMKeyValueStoreFactories<K, V>: TestGenerator<LSMKeyValueStoreFactory<K, V>>

interface StringStringLSMKeyValueStoreFactories: LSMKeyValueStoreFactories<String, String>

interface LongByteArrayLSMKeyValueStoreFactories: LSMKeyValueStoreFactories<Long, ByteArray>

internal val keyValueStoreFactoriesModule = module {

    single<StringStringLSMKeyValueStoreFactories> {
        DelegateStringStringLSMKeyValueStoreFactories(
            GenericLSMKeyValueStoreFactories(
                get(),
                Tombstone.string
            )
        )
    }

    single<LongByteArrayLSMKeyValueStoreFactories> {
        DelegateLongByteArrayLSMKeyValueStoreFactories(
            GenericLSMKeyValueStoreFactories(
                get(),
                Tombstone.byte
            )
        )
    }
}

private class DelegateStringStringLSMKeyValueStoreFactories(private val delegate: LSMKeyValueStoreFactories<String, String>):
    StringStringLSMKeyValueStoreFactories, LSMKeyValueStoreFactories<String, String> by delegate

private class DelegateLongByteArrayLSMKeyValueStoreFactories(private val delegate: LSMKeyValueStoreFactories<Long, ByteArray>):
    LongByteArrayLSMKeyValueStoreFactories, LSMKeyValueStoreFactories<Long, ByteArray> by delegate

private class GenericLSMKeyValueStoreFactories<K, V>(private val dispatcher: CoroutineDispatcher,
                                                     private val tombstone: V): LSMKeyValueStoreFactories<K, V> {

    override fun generate(): Sequence<TestInstance<LSMKeyValueStoreFactory<K, V>>> = sequenceOf(
        TestInstance("${LSMKeyValueStoreFactory::class.simpleName} with Tombstone '$tombstone' and $dispatcher") {
            LSMKeyValueStoreFactory(tombstone, dispatcher)
        }
    )
}