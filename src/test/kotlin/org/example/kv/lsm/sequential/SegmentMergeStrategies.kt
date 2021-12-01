package org.example.kv.lsm.sequential

import io.kotest.property.Gen
import org.example.kv.lsm.*
import org.example.kv.segmentThreshold
import org.example.map
import org.example.size.ByteArraySizeCalculator
import org.example.size.LongSizeCalculator
import org.example.size.SizeCalculator
import org.example.size.StringSizeCalculator
import org.koin.dsl.module

private fun <K, V> segmentMergeStrategy(
    segmentFactories: Gen<SegmentFactory<K, V>>,
    keySizeCalculator: SizeCalculator<K>,
    valueSizeCalculator: SizeCalculator<V>,
) = segmentFactories.map { SequentialLogMergeStrategy(
    it, segmentThreshold, keySizeCalculator, valueSizeCalculator
) }

internal val segmentMergeStrategiesModule = module {

    singleSequentialQ { StringStringSegmentMergeStrategies(
        segmentMergeStrategy(
            get<StringStringSegmentFactories>().gen,
            get<StringSizeCalculator>(),
            get<StringSizeCalculator>(),
        )
    ) }

    singleSequentialQ { LongByteArraySegmentMergeStrategies(
        segmentMergeStrategy(
            get<LongByteArraySegmentFactories>().gen,
            get<LongSizeCalculator>(),
            get<ByteArraySizeCalculator>(),
        )
    ) }

}
