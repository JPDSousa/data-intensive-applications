package org.example.kv.lsm.sequential

import org.example.TestInstance
import org.example.kv.lsm.*
import org.example.kv.lsm.DelegateStringStringSegmentMergeStrategies
import org.example.kv.segmentThreshold
import org.example.size.ByteArraySizeCalculator
import org.example.size.LongSizeCalculator
import org.example.size.SizeCalculator
import org.example.size.StringSizeCalculator
import org.koin.dsl.module

private class GenericSegmentMergeStrategies<K, V>(
    private val segmentFactories: SegmentFactories<K, V>,
    private val keySizeCalculator: SizeCalculator<K>,
    private val valueSizeCalculator: SizeCalculator<V>,
): SegmentMergeStrategies<K, V> {

    override fun generate(): Sequence<TestInstance<SegmentMergeStrategy<K, V>>> = sequence {

        for (segmentFactory in segmentFactories) {

            yield(TestInstance("${SequentialLogMergeStrategy::class.simpleName} with $segmentFactory") {

                SequentialLogMergeStrategy(
                    segmentFactory.instance(),
                    segmentThreshold,
                    keySizeCalculator,
                    valueSizeCalculator
                )
            })
        }
    }
}

internal val segmentMergeStrategiesModule = module {

    singleSequentialQ<StringStringSegmentMergeStrategies> {
        DelegateStringStringSegmentMergeStrategies(
            GenericSegmentMergeStrategies(
                get<StringStringSegmentFactories>(),
                get<StringSizeCalculator>(),
                get<StringSizeCalculator>(),
            )
        )
    }

    singleSequentialQ<LongByteArraySegmentMergeStrategies> {
        DelegateLongByteArraySegmentMergeStrategies(
            GenericSegmentMergeStrategies(
                get<LongByteArraySegmentFactories>(),
                get<LongSizeCalculator>(),
                get<ByteArraySizeCalculator>(),
            )
        )
    }
}
