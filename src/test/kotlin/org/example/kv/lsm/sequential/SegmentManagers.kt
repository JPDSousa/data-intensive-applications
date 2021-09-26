package org.example.kv.lsm.sequential

import org.example.TestGeneratorAdapter
import org.example.TestInstance
import org.example.TestResources
import org.example.encoder.*
import org.example.generator.CompositeGenerator
import org.example.kv.*
import org.example.kv.lsm.*
import org.example.log.*
import org.example.size.ByteArraySizeCalculator
import org.example.size.LongSizeCalculator
import org.example.size.SizeCalculator
import org.example.size.StringSizeCalculator
import org.koin.dsl.module


val segmentManagersModule = module {

    single<StringStringSegmentManagers> {
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

    single<LongByteArraySegmentManagers> {
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

private class Generic<K, V, E>(
    private val logFactories: LogFactories<E>,
    private val logEncoders: Encoders<Map.Entry<K, V>, E>,
    private val keySizeCalculator: SizeCalculator<K>,
    private val valueSizeCalculator: SizeCalculator<V>,
    private val segmentKVFactories: LogKeyValueStoreFactories<K, V>,
    private val resources: TestResources
): SegmentManagers<K, V> {

    override fun generate(): Sequence<TestInstance<SequentialSegmentManager<K, V>>> = sequence {

        for (logFactory in logFactories.generate()) {

            for (logEncoder in logEncoders.generate()) {

                for (segmentKVFactory in segmentKVFactories.generate()) {

                    yield(TestInstance("LSM Segment Manager ~ ${logEncoder.name} ~ ${segmentKVFactory.name} " +
                            "~ ${logFactory.name}") {

                        sequentialSegmentManager(logFactory, logEncoder, segmentKVFactory)
                    })
                }

            }

        }
    }

    private fun sequentialSegmentManager(logFactory: TestInstance<LogFactory<E>>,
                                         logEncoder: TestInstance<Encoder<Map.Entry<K, V>, E>>,
                                         segmentKVFactory: TestInstance<LogKeyValueStoreFactory<K, V>>
    ): SequentialSegmentManager<K, V> {
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
        return SequentialSegmentManager(
            segmentDirectory,
            pairLogFactory,
            logKVFactory,
            segmentThreshold,
            mergeStrategy
        )
    }
}