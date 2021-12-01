package org.example.kv.lsm

import io.kotest.property.Gen
import org.example.GenWrapper
import org.example.getAllGen
import org.koin.dsl.module

data class StringStringSegmentMergeStrategies(
    override val gen: Gen<SegmentMergeStrategy<String, String>>
) : GenWrapper<SegmentMergeStrategy<String, String>>
data class LongByteArraySegmentMergeStrategies(
    override val gen: Gen<SegmentMergeStrategy<Long, ByteArray>>
) : GenWrapper<SegmentMergeStrategy<Long, ByteArray>>

val lsmSegmentMergeStrategies = module {

    single { StringStringSegmentMergeStrategies(
        getAllGen<SegmentMergeStrategy<String, String>, StringStringSegmentMergeStrategies>(qualifiers)
    ) }
    single { LongByteArraySegmentMergeStrategies(
        getAllGen<SegmentMergeStrategy<Long, ByteArray>, LongByteArraySegmentMergeStrategies>(qualifiers)
    ) }

}