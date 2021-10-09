package org.example.kv.lsm

import org.example.TestGenerator
import org.example.TestGeneratorAdapter
import org.example.TestInstance
import org.example.generator.CompositeGenerator
import org.example.kv.LogKeyValueStoreFactories
import org.example.kv.LongByteArrayLogKeyValueStoreFactories
import org.example.kv.StringStringLogKeyValueStoreFactories
import org.example.kv.segmentThreshold
import org.example.log.LogFactories
import org.example.log.LongByteArrayMapEntryLogFactories
import org.example.log.StringStringMapEntryLogFactories
import org.koin.dsl.module

interface SegmentManagers<K, V>: TestGenerator<SegmentManager<K, V>>

interface StringStringSegmentManagers: SegmentManagers<String, String>
interface LongByteArraySegmentManagers: SegmentManagers<Long, ByteArray>

private class DelegateStringString(private val delegate: TestGenerator<SegmentManager<String, String>>)
    : StringStringSegmentManagers, SegmentManagers<String, String>, TestGenerator<SegmentManager<String, String>> by delegate

private class DelegateLongByteArray(private val delegate: TestGenerator<SegmentManager<Long, ByteArray>>)
    : LongByteArraySegmentManagers, SegmentManagers<Long, ByteArray>, TestGenerator<SegmentManager<Long, ByteArray>> by delegate

fun stringStringSegmentManager(managers: Iterable<TestGenerator<SegmentManager<String, String>>>)
        : StringStringSegmentManagers = DelegateStringString(
    TestGeneratorAdapter(
        CompositeGenerator(
            managers
        )
    )
)

fun longByteArraySegmentManager(managers: Iterable<TestGenerator<SegmentManager<Long, ByteArray>>>)
        : LongByteArraySegmentManagers = DelegateLongByteArray(
    TestGeneratorAdapter(
        CompositeGenerator(
            managers
        )
    )
)

private class GenericSegmentManagers<K: Comparable<K>, V>(
    private val logFactories: LogFactories<Map.Entry<K, V>>,
    private val segmentKVFactories: LogKeyValueStoreFactories<K, V>,
    private val openSegmentFactories: OpenSegmentFactories<K, V>,
    private val segmentMergeStrategies: SegmentMergeStrategies<K, V>,
    private val segmentDirectories: SegmentDirectories
): SegmentManagers<K, V> {

    override fun generate(): Sequence<TestInstance<SegmentManager<K, V>>> = sequence {

        for (logFactory in logFactories) {

            for (segmentKVFactory in segmentKVFactories) {

                for (openSegmentFactory in openSegmentFactories) {

                    for (segmentMergeStrategy in segmentMergeStrategies) {

                        for (segmentDirectory in segmentDirectories) {

                            val instanceName = "${SegmentManager::class.simpleName} with $logFactory, " +
                                    "$segmentKVFactory, $openSegmentFactory, $segmentMergeStrategy and " +
                                    "$segmentDirectory"
                            yield(TestInstance(instanceName) {

                                SegmentManager(
                                    openSegmentFactory.instance(),
                                    segmentDirectory.instance(),
                                    logFactory.instance(),
                                    segmentKVFactory.instance(),
                                    segmentMergeStrategy.instance(),
                                    segmentThreshold
                                )
                            })
                        }
                    }
                }
            }
        }
    }
}

val segmentManagersModule = module {

    single {
        stringStringSegmentManager(listOf(
            GenericSegmentManagers(
                get<StringStringMapEntryLogFactories>(),
                get<StringStringLogKeyValueStoreFactories>(),
                get<StringStringOpenSegmentFactories>(),
                get<StringStringSegmentMergeStrategies>(),
                get(),
            ),
        ))
    }

    single {
        longByteArraySegmentManager(listOf(
            GenericSegmentManagers(
                get<LongByteArrayMapEntryLogFactories>(),
                get<LongByteArrayLogKeyValueStoreFactories>(),
                get<LongByteArrayOpenSegmentFactories>(),
                get<LongByteArraySegmentMergeStrategies>(),
                get()
            ),
        ))
    }

}
