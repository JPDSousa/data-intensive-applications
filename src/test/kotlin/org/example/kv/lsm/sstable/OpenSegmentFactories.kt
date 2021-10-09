package org.example.kv.lsm.sstable

import org.example.TestInstance
import org.example.kv.LogKeyValueStoreFactories
import org.example.kv.LongByteArrayLogKeyValueStoreFactories
import org.example.kv.StringStringLogKeyValueStoreFactories
import org.example.kv.lsm.*
import org.example.kv.segmentThreshold
import org.example.log.LogFactories
import org.example.log.LongByteArrayMapEntryLogFactories
import org.example.log.StringStringMapEntryLogFactories
import org.koin.dsl.module

private class SSTableOpenSegmentFactories<K: Comparable<K>, V>(
    private val segmentDirectories: SegmentDirectories,
    private val memTableFactories: MemTableFactories<K, V>,
    private val logFactories: LogFactories<Map.Entry<K, V>>,
    private val logKeyValueStoreFactories: LogKeyValueStoreFactories<K, V>,
): OpenSegmentFactories<K, V> {

    override fun generate(): Sequence<TestInstance<OpenSegmentFactory<K, V>>> = sequence {

        for (segmentDirectory in segmentDirectories) {
            for (logFactory in logFactories) {
                for (logKeyValueStoreFactory in logKeyValueStoreFactories) {
                    for (memTableFactory in memTableFactories) {

                        yield(TestInstance("${OpenSegmentFactory::class.simpleName} with $segmentDirectory, " +
                                "$logFactory, $logKeyValueStoreFactory and $memTableFactory") {

                            SSTableOpenSegmentFactory(
                                segmentDirectory.instance(),
                                memTableFactory.instance(),
                                logFactory.instance(),
                                logKeyValueStoreFactory.instance(),
                                segmentThreshold
                            )
                        })
                    }
                }
            }
        }
    }
}

val sstableOpenSegmentFactories = module {

    singleSSTableQ<StringStringOpenSegmentFactories> {
        DelegateStringStringOpenSegmentFactories(
            SSTableOpenSegmentFactories(
                get(),
                get<StringStringMemTableFactories>(),
                get<StringStringMapEntryLogFactories>(),
                get<StringStringLogKeyValueStoreFactories>()
            )
        )
    }

    singleSSTableQ<LongByteArrayOpenSegmentFactories> {
        DelegateLongByteArrayOpenSegmentFactories(
            SSTableOpenSegmentFactories(
                get(),
                get<LongByteArrayMemTableFactories>(),
                get<LongByteArrayMapEntryLogFactories>(),
                get<LongByteArrayLogKeyValueStoreFactories>()
            )
        )
    }

}
