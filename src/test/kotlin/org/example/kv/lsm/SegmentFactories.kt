package org.example.kv.lsm

import org.example.TestGenerator
import org.example.TestInstance
import org.example.kv.LogKeyValueStoreFactories
import org.example.kv.LongByteArrayLogKeyValueStoreFactories
import org.example.kv.StringStringLogKeyValueStoreFactories
import org.example.kv.segmentThreshold
import org.example.log.LogFactories
import org.example.log.LongByteArrayMapEntryLogFactories
import org.example.log.StringStringMapEntryLogFactories
import org.koin.dsl.module

interface SegmentFactories<K, V>: TestGenerator<SegmentFactory<K, V>>

interface StringStringSegmentFactories: SegmentFactories<String, String>
interface LongByteArraySegmentFactories: SegmentFactories<Long, ByteArray>

private class DelegateStringStringSegmentFactories(private val delegate: SegmentFactories<String, String>)
    : StringStringSegmentFactories, SegmentFactories<String, String> by delegate

private class DelegateLongByteArraySegmentFactories(private val delegate: SegmentFactories<Long, ByteArray>)
    : LongByteArraySegmentFactories, SegmentFactories<Long, ByteArray> by delegate

private class GenericSegmentFactories<K, V>(
    private val segmentDirectories: SegmentDirectories,
    private val logFactories: LogFactories<Map.Entry<K, V>>,
    private val logKVFactories: LogKeyValueStoreFactories<K, V>
): SegmentFactories<K, V> {

    override fun generate(): Sequence<TestInstance<SegmentFactory<K, V>>> = sequence {

        for(segmentDirectory in segmentDirectories) {

            for(logFactory in logFactories) {

                for(logKVFactory in logKVFactories) {

                    val instanceName = "${SegmentFactory::class.simpleName} with $segmentDirectory, $logFactory " +
                            "and $logKVFactory;"
                    yield(TestInstance(instanceName) {
                        SegmentFactory(
                            segmentDirectory.instance(),
                            logFactory.instance(),
                            logKVFactory.instance(),
                            segmentThreshold
                        )
                    })
                }
            }
        }
    }
}

val segmentFactoriesModule = module {

    single<StringStringSegmentFactories> {
        DelegateStringStringSegmentFactories(
            GenericSegmentFactories(
                get(),
                get<StringStringMapEntryLogFactories>(),
                get<StringStringLogKeyValueStoreFactories>()
            )
        )
    }

    single<LongByteArraySegmentFactories> {
        DelegateLongByteArraySegmentFactories(
            GenericSegmentFactories(
                get(),
                get<LongByteArrayMapEntryLogFactories>(),
                get<LongByteArrayLogKeyValueStoreFactories>()
            )
        )
    }

}
