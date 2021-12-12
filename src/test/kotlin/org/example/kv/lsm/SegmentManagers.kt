package org.example.kv.lsm

import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.arbitrary.bind
import org.example.GenWrapper
import org.example.kv.LogKeyValueStoreFactory
import org.example.kv.LongByteArrayLogKeyValueStoreFactories
import org.example.kv.StringStringLogKeyValueStoreFactories
import org.example.kv.segmentThreshold
import org.example.log.EntryLogFactory
import org.example.log.LongByteArrayMapEntryLogFactories
import org.example.log.StringStringMapEntryLogFactories
import org.koin.dsl.module

data class StringStringSegmentManagers(
    override val gen: Gen<SegmentManager<String, String>>
) : GenWrapper<SegmentManager<String, String>>
data class LongByteArraySegmentManagers(
    override val gen: Gen<SegmentManager<Long, ByteArray>>
) : GenWrapper<SegmentManager<Long, ByteArray>>

fun <K, V> segmentManagers(
    logFactories: Gen<EntryLogFactory<K, V>>,
    segmentKVFactories: Gen<LogKeyValueStoreFactory<K, V>>,
    openSegmentFactories: Gen<OpenSegmentFactory<K, V>>,
    segmentMergeStrategies: Gen<SegmentMergeStrategy<K, V>>,
    segmentDirectories: Gen<SegmentDirectory>
) = Arb.bind(
    logFactories,
    segmentKVFactories,
    openSegmentFactories,
    segmentMergeStrategies,
    segmentDirectories
) { logFactory, segmentKVFactory, openSegmentFactory, segmentMergeStrategy, segmentDirectory ->
    SegmentManager(
        openSegmentFactory,
        segmentDirectory,
        logFactory,
        segmentKVFactory,
        segmentMergeStrategy,
        segmentThreshold
    )
}

val segmentManagersModule = module {

    single { StringStringSegmentManagers(segmentManagers(
        get<StringStringMapEntryLogFactories>().gen,
        get<StringStringLogKeyValueStoreFactories>().gen,
        get<StringStringOpenSegmentFactories>().gen,
        get<StringStringSegmentMergeStrategies>().gen,
        get<SegmentDirectories>().gen,
    )) }

    single { LongByteArraySegmentManagers(segmentManagers(
        get<LongByteArrayMapEntryLogFactories>().gen,
        get<LongByteArrayLogKeyValueStoreFactories>().gen,
        get<LongByteArrayOpenSegmentFactories>().gen,
        get<LongByteArraySegmentMergeStrategies>().gen,
        get<SegmentDirectories>().gen,
    )) }

}
