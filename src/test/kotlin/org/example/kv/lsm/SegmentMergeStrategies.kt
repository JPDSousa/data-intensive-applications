package org.example.kv.lsm

import org.example.TestGenerator
import org.example.TestGeneratorAdapter
import org.example.generator.CompositeGenerator
import org.example.mergeGenerators
import org.koin.core.module.Module
import org.koin.core.qualifier.Qualifier
import org.koin.dsl.module

interface SegmentMergeStrategies<K, V>: TestGenerator<SegmentMergeStrategy<K, V>>

interface StringStringSegmentMergeStrategies: SegmentMergeStrategies<String, String>
interface LongByteArraySegmentMergeStrategies: SegmentMergeStrategies<Long, ByteArray>

internal class DelegateStringStringSegmentMergeStrategies(
    private val delegate: TestGenerator<SegmentMergeStrategy<String, String>>)
    : StringStringSegmentMergeStrategies, TestGenerator<SegmentMergeStrategy<String, String>> by delegate

internal class DelegateLongByteArraySegmentMergeStrategies(
    private val delegate: TestGenerator<SegmentMergeStrategy<Long, ByteArray>>)
    : LongByteArraySegmentMergeStrategies, TestGenerator<SegmentMergeStrategy<Long, ByteArray>> by delegate

val lsmSegmentMergeStrategies = module {

    mergeGenerators<SegmentMergeStrategy<String, String>, StringStringSegmentMergeStrategies>(qualifiers) {
        DelegateStringStringSegmentMergeStrategies(it)
    }

    mergeGenerators<SegmentMergeStrategy<Long, ByteArray>, LongByteArraySegmentMergeStrategies>(qualifiers) {
        DelegateLongByteArraySegmentMergeStrategies(it)
    }

}