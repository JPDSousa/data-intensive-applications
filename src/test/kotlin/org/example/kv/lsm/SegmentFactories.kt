package org.example.kv.lsm

import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.arbitrary.bind
import org.example.GenWrapper
import org.example.kv.LogKeyValueStoreFactory
import org.example.kv.LongByteArrayLogKeyValueStoreFactories
import org.example.kv.StringStringLogKeyValueStoreFactories
import org.example.kv.segmentThreshold
import org.example.log.LogFactory
import org.example.log.LongByteArrayMapEntryLogFactories
import org.example.log.StringStringMapEntryLogFactories
import org.koin.core.module.Module
import org.koin.dsl.module

data class StringStringSegmentFactories(
    override val gen: Gen<SegmentFactory<String, String>>
) : GenWrapper<SegmentFactory<String, String>>
data class LongByteArraySegmentFactories(
    override val gen: Gen<SegmentFactory<Long, ByteArray>>
) : GenWrapper<SegmentFactory<Long, ByteArray>>

private inline fun <K, V, reified R, reified L, reified LKV> Module.singleSegmentFactories(
    crossinline operation: (Gen<SegmentFactory<K, V>>) -> R
) where R:  GenWrapper<SegmentFactory<K, V>>,
        L: GenWrapper<LogFactory<Map.Entry<K, V>>>,
        LKV: GenWrapper<LogKeyValueStoreFactory<K, V>> {
    single { operation(Arb.bind(
        get<L>().gen,
        get<LKV>().gen,
        get<SegmentDirectories>().gen
    ) { logFactory, kvFactory, segmentDirectory ->
        SegmentFactory(
            segmentDirectory,
            logFactory,
            kvFactory,
            segmentThreshold,
        )
    }) }
}

val segmentFactoriesModule = module {

    singleSegmentFactories<String, String, StringStringSegmentFactories, StringStringMapEntryLogFactories, StringStringLogKeyValueStoreFactories> {
        StringStringSegmentFactories(it)
    }

    singleSegmentFactories<Long, ByteArray, LongByteArraySegmentFactories, LongByteArrayMapEntryLogFactories, LongByteArrayLogKeyValueStoreFactories> {
        LongByteArraySegmentFactories(it)
    }

}
