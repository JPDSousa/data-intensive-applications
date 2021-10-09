package org.example.kv.lsm.sstable

import org.example.TestInstance
import org.example.kv.lsm.*
import org.koin.dsl.module

private class GenericSegmentMergeStrategies<K: Comparable<K>, V>(
    private val segmentFactories: SegmentFactories<K, V>
): SegmentMergeStrategies<K, V> {

    override fun generate(): Sequence<TestInstance<SegmentMergeStrategy<K, V>>> = sequence {

        for (segmentFactory in segmentFactories) {

            yield(TestInstance("${SSTableMergeStrategy::class.simpleName} with $segmentFactory") {
                SSTableMergeStrategy(
                    segmentFactory.instance()
                )
            })
        }

    }
}

internal val segmentMergeStrategiesModule = module {

    singleSSTableQ<StringStringSegmentMergeStrategies> {
        DelegateStringStringSegmentMergeStrategies(
            GenericSegmentMergeStrategies(
                get<StringStringSegmentFactories>()
            )
        )
    }

    singleSSTableQ<LongByteArraySegmentMergeStrategies> {
        DelegateLongByteArraySegmentMergeStrategies(
            GenericSegmentMergeStrategies(
                get<LongByteArraySegmentFactories>()
            )
        )
    }

}
