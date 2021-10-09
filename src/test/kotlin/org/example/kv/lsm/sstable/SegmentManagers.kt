package org.example.kv.lsm.sstable

import org.example.TestInstance
import org.example.TestResources
import org.example.encoder.*
import org.example.kv.*
import org.example.kv.lsm.*
import org.example.log.ByteArrayLogFactories
import org.example.log.LogEncoderFactory
import org.example.log.LogFactories
import org.example.log.StringLogFactories
import org.example.size.ByteArraySizeCalculator
import org.example.size.LongSizeCalculator
import org.example.size.SizeCalculator
import org.example.size.StringSizeCalculator
import org.koin.core.qualifier.named
import org.koin.dsl.module

val sstableQ = named("sstableSegmentManager")

val sstableSegmentManagersModule = module {

    single(sstableQ) {
        stringStringSegmentManager(listOf(
            Generic(
                get<StringLogFactories>(),
                get<StringStringMapEntry2StringEncoders>(),
                get<StringSizeCalculator>(),
                get<StringSizeCalculator>(),
                get<StringStringLogKeyValueStoreFactories>(),
                Tombstone.string,
                get(),
            ),
            Generic(
                get<ByteArrayLogFactories>(),
                get<StringStringMapEntry2ByteArrayEncoders>(),
                get<StringSizeCalculator>(),
                get<StringSizeCalculator>(),
                get<StringStringLogKeyValueStoreFactories>(),
                Tombstone.string,
                get()
            ),
        ))
    }

    single(sstableQ) {
        longByteArraySegmentManager(listOf(
            Generic(
                get<StringLogFactories>(),
                get<LongByteArrayMapEntry2StringEncoders>(),
                get<LongSizeCalculator>(),
                get<ByteArraySizeCalculator>(),
                get<LongByteArrayLogKeyValueStoreFactories>(),
                Tombstone.byte,
                get()
            ),
            Generic(
                get<ByteArrayLogFactories>(),
                get<LongByteArrayMapEntry2ByteArrayEncoders>(),
                get<LongSizeCalculator>(),
                get<ByteArraySizeCalculator>(),
                get<LongByteArrayLogKeyValueStoreFactories>(),
                Tombstone.byte,
                get()
            ),
        ))
    }
}

private class Generic<K: Comparable<K>, V, E>(
    private val logFactories: LogFactories<E>,
    private val logEncoders: Encoders<Map.Entry<K, V>, E>,
    private val keySizeCalculator: SizeCalculator<K>,
    private val valueSizeCalculator: SizeCalculator<V>,
    private val segmentKVFactories: LogKeyValueStoreFactories<K, V>,
    private val tombstone: V,
    private val resources: TestResources): SegmentManagers<K, V> {

    override fun generate(): Sequence<TestInstance<SegmentManager<K, V>>> = sequence {

        for (logFactory in logFactories) {

            for (logEncoder in logEncoders) {

                for (segmentKVFactory in segmentKVFactories) {

                    yield(TestInstance("SSTable SegmentManager") {

                        val pairLogFactory = LogEncoderFactory(logFactory.instance(), logEncoder.instance())
                        val segmentDirectory = SegmentDirectory(resources.allocateTempDir("segmented-"))
                        val logKVFactory = segmentKVFactory.instance()

                        val segmentFactory = SegmentFactory(
                            segmentDirectory,
                            pairLogFactory,
                            logKVFactory,
                            segmentThreshold
                        )

                        val mergeStrategy = SSTableMergeStrategy(segmentFactory)

                        SSTableSegmentManager(
                            segmentDirectory,
                            pairLogFactory,
                            logKVFactory,
                            mergeStrategy,
                            segmentThreshold,
                            keySizeCalculator,
                            valueSizeCalculator,
                            tombstone
                        )
                    })
                }
            }
        }
    }
}




