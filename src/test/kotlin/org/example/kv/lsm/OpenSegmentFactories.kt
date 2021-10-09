package org.example.kv.lsm

import org.example.TestGenerator
import org.example.mergeGenerators
import org.koin.dsl.module

interface OpenSegmentFactories<K, V>: TestGenerator<OpenSegmentFactory<K, V>>

interface StringStringOpenSegmentFactories: OpenSegmentFactories<String, String>

interface LongByteArrayOpenSegmentFactories: OpenSegmentFactories<Long, ByteArray>

class DelegateStringStringOpenSegmentFactories(private val delegate: TestGenerator<OpenSegmentFactory<String, String>>)
    : StringStringOpenSegmentFactories, TestGenerator<OpenSegmentFactory<String, String>> by delegate

class DelegateLongByteArrayOpenSegmentFactories(private val delegate: TestGenerator<OpenSegmentFactory<Long, ByteArray>>)
    : LongByteArrayOpenSegmentFactories, TestGenerator<OpenSegmentFactory<Long, ByteArray>> by delegate

internal val openSegmentFactoriesModule = module {

    mergeGenerators<OpenSegmentFactory<String, String>, StringStringOpenSegmentFactories>(qualifiers) {
        DelegateStringStringOpenSegmentFactories(it)
    }

    mergeGenerators<OpenSegmentFactory<Long, ByteArray>, LongByteArrayOpenSegmentFactories>(qualifiers) {
        DelegateLongByteArrayOpenSegmentFactories(it)
    }
}
