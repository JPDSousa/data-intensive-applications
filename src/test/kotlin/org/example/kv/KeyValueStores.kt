package org.example.kv

import org.example.TestGenerator
import org.example.TestInstance
import org.example.kv.lsm.LSMKeyValueStoreFactories
import org.example.kv.lsm.LongByteArrayLSMKeyValueStoreFactories
import org.example.kv.lsm.StringStringLSMKeyValueStoreFactories
import org.example.kv.lsm.sequential.LongByteArraySequentialSegmentManagers
import org.example.kv.lsm.sequential.SequentialSegmentManagers
import org.example.kv.lsm.sequential.StringStringSequentialSegmentManagers
import org.koin.dsl.module

val keyValueStoresModule = module {

    single<ByteArrayKeyValueStores> {
        ByteArrayDelegate(
            GenericKeyValueStores(
                get<LongByteArraySequentialSegmentManagers>(),
                get<LongByteArrayLSMKeyValueStoreFactories>()
            )
        )
    }

    single<StringStringKeyValueStores> {
        StringDelegate(
            GenericKeyValueStores(
                get<StringStringSequentialSegmentManagers>(),
                get<StringStringLSMKeyValueStoreFactories>()
            )
        )
    }
}

interface KeyValueStores<K, V>: TestGenerator<KeyValueStore<K, V>>
interface ByteArrayKeyValueStores: KeyValueStores<Long, ByteArray>
interface StringStringKeyValueStores: KeyValueStores<String, String>

private class ByteArrayDelegate(private val delegate: KeyValueStores<Long, ByteArray>):
    ByteArrayKeyValueStores, KeyValueStores<Long, ByteArray> by delegate

private class StringDelegate(private val delegate: KeyValueStores<String, String>):
    StringStringKeyValueStores, KeyValueStores<String, String> by delegate

private class GenericKeyValueStores<K, V>(private val segmentManagers: SequentialSegmentManagers<K, V>,
                                          private val factories: LSMKeyValueStoreFactories<K, V>
): KeyValueStores<K, V> {

    override fun generate(): Sequence<TestInstance<KeyValueStore<K, V>>> = sequence {

        for (factory in factories.generate()) {

            for (segmentManager in segmentManagers.generate()) {

                yield(TestInstance("LSM Key Value Store created from ${factory.name} using Segment Manager " +
                        segmentManager.name
                ) {
                    factory.instance().createLSMKeyValueStore(segmentManager.instance())
                })
            }
        }
    }
}


const val segmentThreshold: Long = 1024 * 1024
