package org.example.kv.lsm.sstable

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
import org.example.kv.segmentThreshold
import org.example.log.LogFactory
import org.example.log.LongByteArrayMapEntryLogFactories
import org.example.log.StringStringMapEntryLogFactories
import org.koin.dsl.module

fun <K: Comparable<K>, V> sstableOpenSegmentFactories(
    segmentDirectories: Gen<SegmentDirectory>,
    memTableFactories: Gen<MemTableFactory<K, V>>,
    logFactories: Gen<LogFactory<Map.Entry<K, V>>>,
    logKeyValueStoreFactories: Gen<LogKeyValueStoreFactory<K, V>>,
) = Arb.bind(
    segmentDirectories,
    memTableFactories,
    logFactories,
    logKeyValueStoreFactories
) { segmentDirectory, memTableFactory, logFactory, logKeyValueStoreFactory ->
    SSTableOpenSegmentFactory(
        segmentDirectory,
        memTableFactory,
        logFactory,
        logKeyValueStoreFactory,
        segmentThreshold
    )
}

val sstableOpenSegmentFactories = module {

    singleSSTableQ { StringStringOpenSegmentFactories(
        sstableOpenSegmentFactories(
            get<SegmentDirectories>().gen,
            get<StringStringMemTableFactories>().gen,
            get<StringStringMapEntryLogFactories>().gen,
            get<StringStringLogKeyValueStoreFactories>().gen
    )) }

    singleSSTableQ { LongByteArrayOpenSegmentFactories(
        sstableOpenSegmentFactories(
            get<SegmentDirectories>().gen,
            get<LongByteArrayMemTableFactories>().gen,
            get<LongByteArrayMapEntryLogFactories>().gen,
            get<LongByteArrayLogKeyValueStoreFactories>().gen
        )) }
}
