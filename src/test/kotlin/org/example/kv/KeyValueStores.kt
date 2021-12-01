package org.example.kv

import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.arbitrary.bind
import org.example.GenWrapper
import org.example.kv.lsm.*
import org.koin.core.module.Module
import org.koin.dsl.module

internal val keyValueStoresModule = module {

    keyValueStores<Long, ByteArray, LongByteArraySegmentManagers, LongByteArrayLSMKeyValueStoreFactories, LongByteArrayKeyValueStores>(
        ::LongByteArrayKeyValueStores
    )
    keyValueStores<String, String, StringStringSegmentManagers, StringStringLSMKeyValueStoreFactories, StringStringKeyValueStores>(
        ::StringStringKeyValueStores
    )
}

data class LongByteArrayKeyValueStores(
    override val gen: Gen<KeyValueStore<Long, ByteArray>>
) : GenWrapper<KeyValueStore<Long, ByteArray>>
data class StringStringKeyValueStores(
    override val gen: Gen<KeyValueStore<String, String>>
) : GenWrapper<KeyValueStore<String, String>>

private inline fun <K, V, reified SM, reified F, reified R> Module.keyValueStores(
    crossinline ins: (Gen<KeyValueStore<K, V>>) -> R
) where SM: GenWrapper<SegmentManager<K, V>>,
        R: GenWrapper<KeyValueStore<K, V>>,
        F: GenWrapper<LSMKeyValueStoreFactory<K, V>> {
    single { ins(Arb.bind(get<SM>().gen, get<F>().gen) { segmentManager, factory ->
        factory.createLSMKeyValueStore(segmentManager)
    }) }
}

const val segmentThreshold: Long = 1024L * 1024L
