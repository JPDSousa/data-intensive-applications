package org.example.kv.lsm

import io.kotest.property.Gen
import org.example.GenWrapper
import org.example.getAllGen
import org.koin.dsl.module

data class StringStringOpenSegmentFactories(
    override val gen: Gen<OpenSegmentFactory<String, String>>
) : GenWrapper<OpenSegmentFactory<String, String>>
data class LongByteArrayOpenSegmentFactories(
    override val gen: Gen<OpenSegmentFactory<Long, ByteArray>>
) : GenWrapper<OpenSegmentFactory<Long, ByteArray>>

internal val openSegmentFactoriesModule = module {

    single { StringStringOpenSegmentFactories(
        getAllGen<OpenSegmentFactory<String, String>, StringStringOpenSegmentFactories>(qualifiers)
    ) }
    single { LongByteArrayOpenSegmentFactories(
        getAllGen<OpenSegmentFactory<Long, ByteArray>, LongByteArrayOpenSegmentFactories>(qualifiers)
    ) }
}
