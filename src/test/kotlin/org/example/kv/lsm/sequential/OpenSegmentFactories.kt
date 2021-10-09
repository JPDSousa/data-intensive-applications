package org.example.kv.lsm.sequential

import org.example.TestInstance
import org.example.kv.LogKeyValueStoreFactories
import org.example.kv.LongByteArrayLogKeyValueStoreFactories
import org.example.kv.StringStringLogKeyValueStoreFactories
import org.example.kv.lsm.*
import org.example.log.LogFactories
import org.example.log.LongByteArrayMapEntryLogFactories
import org.example.log.StringStringMapEntryLogFactories
import org.koin.dsl.module

private class GenericOpenSegmentFactories<K, V>(
    private val logFactories: LogFactories<Map.Entry<K, V>>,
    private val segmentKVFactories: LogKeyValueStoreFactories<K, V>,
    private val segmentDirectories: SegmentDirectories
): OpenSegmentFactories<K, V> {

    override fun generate(): Sequence<TestInstance<OpenSegmentFactory<K, V>>> = sequence {

        for (segmentDirectory in segmentDirectories) {

            for (logFactory in logFactories) {

                for (segmentKVFactory in segmentKVFactories) {

                    yield(TestInstance("${SequentialOpenSegmentFactory::class.simpleName} with $segmentDirectory, $logFactory, " +
                            "$segmentKVFactory") {

                        SequentialOpenSegmentFactory(
                            segmentDirectory.instance(),
                            logFactory.instance(),
                            segmentKVFactory.instance(),
                        )
                    })
                }
            }
        }

    }
}

val sequentialOpenSegmentFactories = module {

    singleSequentialQ<StringStringOpenSegmentFactories> {
        DelegateStringStringOpenSegmentFactories(
            GenericOpenSegmentFactories(
                get<StringStringMapEntryLogFactories>(),
                get<StringStringLogKeyValueStoreFactories>(),
                get(),
            )
        )
    }

    singleSequentialQ<LongByteArrayOpenSegmentFactories> {
        DelegateLongByteArrayOpenSegmentFactories(
            GenericOpenSegmentFactories(
                get<LongByteArrayMapEntryLogFactories>(),
                get<LongByteArrayLogKeyValueStoreFactories>(),
                get(),
            )
        )
    }

}
