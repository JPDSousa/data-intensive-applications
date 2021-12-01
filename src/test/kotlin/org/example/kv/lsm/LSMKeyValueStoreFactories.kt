package org.example.kv.lsm

import io.kotest.property.Gen
import io.kotest.property.exhaustive.exhaustive
import org.example.GenWrapper
import org.example.kv.Tombstone
import org.koin.dsl.module

data class StringStringLSMKeyValueStoreFactories(
    override val gen: Gen<LSMKeyValueStoreFactory<String, String>>
) : GenWrapper<LSMKeyValueStoreFactory<String, String>>
data class LongByteArrayLSMKeyValueStoreFactories(
    override val gen: Gen<LSMKeyValueStoreFactory<Long, ByteArray>>
) : GenWrapper<LSMKeyValueStoreFactory<Long, ByteArray>>

internal val keyValueStoreFactoriesModule = module {

    single { StringStringLSMKeyValueStoreFactories(
        listOf(LSMKeyValueStoreFactory<String, String>(Tombstone.string, get())).exhaustive()
    ) }

    single { LongByteArrayLSMKeyValueStoreFactories(
        listOf(LSMKeyValueStoreFactory<Long, ByteArray>(Tombstone.byte, get())).exhaustive()
    ) }

}
