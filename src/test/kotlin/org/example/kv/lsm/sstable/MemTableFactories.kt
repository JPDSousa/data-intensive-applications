package org.example.kv.lsm.sstable

import io.kotest.property.Gen
import org.example.GenWrapper
import org.example.kv.LogKeyValueStoreFactory
import org.example.kv.LongByteArrayLogKeyValueStoreFactories
import org.example.kv.StringStringLogKeyValueStoreFactories
import org.example.map
import org.example.size.ByteArraySizeCalculator
import org.example.size.LongSizeCalculator
import org.example.size.SizeCalculator
import org.example.size.StringSizeCalculator
import org.koin.dsl.module

data class StringStringMemTableFactories(
    override val gen: Gen<MemTableFactory<String, String>>
) : GenWrapper<MemTableFactory<String, String>>
data class LongByteArrayMemTableFactories(
    override val gen: Gen<MemTableFactory<Long, ByteArray>>
) : GenWrapper<MemTableFactory<Long, ByteArray>>

private fun <K: Comparable<K>, V> memTableFactories(
    logKeyValueStoreFactories: Gen<LogKeyValueStoreFactory<K, V>>,
    keySize: SizeCalculator<K>,
    valueSize: SizeCalculator<V>
) = logKeyValueStoreFactories.map {
    MemTableFactory(it, keySize, valueSize)
}

val memTableFactoriesModule = module {

    single { StringStringMemTableFactories(
        memTableFactories(
            get<StringStringLogKeyValueStoreFactories>().gen,
            get<StringSizeCalculator>(),
            get<StringSizeCalculator>()
        )
    ) }

    single { LongByteArrayMemTableFactories(
        memTableFactories(
            get<LongByteArrayLogKeyValueStoreFactories>().gen,
            get<LongSizeCalculator>(),
            get<ByteArraySizeCalculator>()
        )
    ) }

}
