package org.example.kv.lsm.sequential

import org.example.TestGenerator
import org.example.TestGeneratorAdapter
import org.example.TestInstance
import org.example.TestResources
import org.example.encoder.*
import org.example.generator.CompositeGenerator
import org.example.kv.LogKeyValueStoreFactories
import org.example.kv.LongByteArrayLogKeyValueStoreFactories
import org.example.kv.StringStringLogKeyValueStoreFactories
import org.example.kv.lsm.SegmentDirectory
import org.example.kv.lsm.SegmentFactory
import org.example.kv.segmentThreshold
import org.example.log.ByteArrayLogFactories
import org.example.log.LogEncoderFactory
import org.example.log.LogFactories
import org.example.log.StringLogFactories
import org.example.size.ByteArraySizeCalculator
import org.example.size.LongSizeCalculator
import org.example.size.SizeCalculator
import org.example.size.StringSizeCalculator
import org.koin.dsl.module


val segmentManagersModule = module {

    single<StringStringSequentialSegmentManagers> {
        DelegateStringString(
            TestGeneratorAdapter(
                CompositeGenerator(
                    listOf(
                        Generic(
                            get<StringLogFactories>(),
                            get<StringStringMapEntry2StringEncoders>(),
                            get<StringSizeCalculator>(),
                            get<StringSizeCalculator>(),
                            get<StringStringLogKeyValueStoreFactories>(),
                            get()
                        ),
                        Generic(
                            get<ByteArrayLogFactories>(),
                            get<StringStringMapEntry2ByteArrayEncoders>(),
                            get<StringSizeCalculator>(),
                            get<StringSizeCalculator>(),
                            get<StringStringLogKeyValueStoreFactories>(),
                            get()
                        ),
                    )
                )
            )
        )
    }

    single<LongByteArraySequentialSegmentManagers> {
        DelegateLongByteArray(
            TestGeneratorAdapter(
                CompositeGenerator(
                    listOf(
                        Generic(
                            get<StringLogFactories>(),
                            get<LongByteArrayMapEntry2StringEncoders>(),
                            get<LongSizeCalculator>(),
                            get<ByteArraySizeCalculator>(),
                            get<LongByteArrayLogKeyValueStoreFactories>(),
                            get()
                        ),
                        Generic(
                            get<ByteArrayLogFactories>(),
                            get<LongByteArrayMapEntry2ByteArrayEncoders>(),
                            get<LongSizeCalculator>(),
                            get<ByteArraySizeCalculator>(),
                            get<LongByteArrayLogKeyValueStoreFactories>(),
                            get()
                        ),
                    )
                )
            )
        )
    }
}


interface SequentialSegmentManagers<K, V>: TestGenerator<SequentialSegmentManager<K, V>>
interface StringStringSequentialSegmentManagers: SequentialSegmentManagers<String, String>
interface LongByteArraySequentialSegmentManagers: SequentialSegmentManagers<Long, ByteArray>

private class DelegateStringString(private val delegate: TestGenerator<SequentialSegmentManager<String, String>>)
    : StringStringSequentialSegmentManagers, SequentialSegmentManagers<String, String>, TestGenerator<SequentialSegmentManager<String, String>> by delegate

private class DelegateLongByteArray(private val delegate: TestGenerator<SequentialSegmentManager<Long, ByteArray>>)
    : LongByteArraySequentialSegmentManagers, SequentialSegmentManagers<Long, ByteArray>, TestGenerator<SequentialSegmentManager<Long, ByteArray>> by delegate

private class Generic<K, V, E>(
    private val logFactories: LogFactories<E>,
    private val logEncoders: Encoders<Map.Entry<K, V>, E>,
    private val keySizeCalculator: SizeCalculator<K>,
    private val valueSizeCalculator: SizeCalculator<V>,
    private val segmentKVFactories: LogKeyValueStoreFactories<K, V>,
    private val resources: TestResources
): SequentialSegmentManagers<K, V> {

    override fun generate(): Sequence<TestInstance<SequentialSegmentManager<K, V>>> = sequence {

        for (logFactory in logFactories.generate()) {

            for (logEncoder in logEncoders.generate()) {

                for (segmentKVFactory in segmentKVFactories.generate()) {

                    yield(TestInstance("Binary Append-only LSM Key Value Store") {

                        val pairLogFactory = LogEncoderFactory(logFactory.instance(), logEncoder.instance())
                        val segmentDirectory = SegmentDirectory(resources.allocateTempDir("segmented-"))
                        val logKVFactory = segmentKVFactory.instance()

                        val segmentFactory = SegmentFactory(
                            segmentDirectory,
                            pairLogFactory,
                            logKVFactory,
                            segmentThreshold
                        )
                        val mergeStrategy = SequentialLogMergeStrategy(
                            segmentFactory,
                            segmentThreshold,
                            keySizeCalculator,
                            valueSizeCalculator
                        )
                        SequentialSegmentManager(
                            segmentDirectory,
                            pairLogFactory,
                            logKVFactory,
                            segmentThreshold,
                            mergeStrategy
                        )
                    })
                }

            }

        }
    }
}