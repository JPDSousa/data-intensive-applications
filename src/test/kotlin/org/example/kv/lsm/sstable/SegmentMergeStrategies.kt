package org.example.kv.lsm.sstable

import io.kotest.property.Gen
import org.example.kv.lsm.*
import org.example.map
import org.koin.dsl.module

private fun <K: Comparable<K>, V> segmentMergeStrategies(
    segmentFactories: Gen<SegmentFactory<K, V>>
) = segmentFactories.map { SSTableMergeStrategy(it) }

internal val segmentMergeStrategiesModule = module {

    singleSSTableQ { StringStringSegmentMergeStrategies(
        segmentMergeStrategies(
            get<StringStringSegmentFactories>().gen
        )
    ) }

    singleSSTableQ { LongByteArraySegmentMergeStrategies(
        segmentMergeStrategies(
            get<LongByteArraySegmentFactories>().gen
        )
    ) }

}
