package org.example.kv.lsm.sequential

import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.arbitrary.bind
import org.example.kv.LogKeyValueStoreFactory
import org.example.kv.LongByteArrayLogKeyValueStoreFactories
import org.example.kv.StringStringLogKeyValueStoreFactories
import org.example.kv.lsm.LongByteArrayOpenSegmentFactories
import org.example.kv.lsm.SegmentDirectories
import org.example.kv.lsm.SegmentDirectory
import org.example.kv.lsm.StringStringOpenSegmentFactories
import org.example.log.LogFactory
import org.example.log.LongByteArrayMapEntryLogFactories
import org.example.log.StringStringMapEntryLogFactories
import org.koin.dsl.module

private fun <K, V> sequentialOpenSegmentFactories(
    logFactories: Gen<LogFactory<Map.Entry<K, V>>>,
    segmentKVFactories: Gen<LogKeyValueStoreFactory<K, V>>,
    segmentDirectories: Gen<SegmentDirectory>
) = Arb.bind(
    segmentDirectories,
    logFactories,
    segmentKVFactories,
    ::SequentialOpenSegmentFactory
)

val sequentialOpenSegmentFactories = module {

    singleSequentialQ { StringStringOpenSegmentFactories(
        sequentialOpenSegmentFactories(
            get<StringStringMapEntryLogFactories>().gen,
            get<StringStringLogKeyValueStoreFactories>().gen,
            get<SegmentDirectories>().gen,
        )
    ) }

    singleSequentialQ { LongByteArrayOpenSegmentFactories(
        sequentialOpenSegmentFactories(
            get<LongByteArrayMapEntryLogFactories>().gen,
            get<LongByteArrayLogKeyValueStoreFactories>().gen,
            get<SegmentDirectories>().gen,
        )
    ) }

}
