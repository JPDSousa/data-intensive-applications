package org.example.kv.lsm

import org.example.TestGenerator
import org.example.TestGeneratorAdapter
import org.example.generator.CompositeGenerator
import org.example.kv.lsm.sequential.sequentialQ
import org.example.kv.lsm.sstable.sstableQ
import org.koin.dsl.module

interface SegmentManagers<K, V>: TestGenerator<SegmentManager<K, V>>

interface StringStringSegmentManagers: SegmentManagers<String, String>
interface LongByteArraySegmentManagers: SegmentManagers<Long, ByteArray>

private class DelegateStringString(private val delegate: TestGenerator<SegmentManager<String, String>>)
    : StringStringSegmentManagers, SegmentManagers<String, String>, TestGenerator<SegmentManager<String, String>> by delegate

private class DelegateLongByteArray(private val delegate: TestGenerator<SegmentManager<Long, ByteArray>>)
    : LongByteArraySegmentManagers, SegmentManagers<Long, ByteArray>, TestGenerator<SegmentManager<Long, ByteArray>> by delegate

fun stringStringSegmentManager(managers: Iterable<TestGenerator<SegmentManager<String, String>>>)
        : StringStringSegmentManagers = DelegateStringString(
    TestGeneratorAdapter(
        CompositeGenerator(
            managers
        )
    )
)

fun longByteArraySegmentManager(managers: Iterable<TestGenerator<SegmentManager<Long, ByteArray>>>)
        : LongByteArraySegmentManagers = DelegateLongByteArray(
    TestGeneratorAdapter(
        CompositeGenerator(
            managers
        )
    )
)

val segmentManagersModule = module {

    single {
        stringStringSegmentManager(listOf(
            get<StringStringSegmentManagers>(sequentialQ),
            get<StringStringSegmentManagers>(sstableQ)
        ))
    }

    single {
        longByteArraySegmentManager(listOf(
            get<LongByteArraySegmentManagers>(sequentialQ),
            get<LongByteArraySegmentManagers>(sstableQ),
        ))
    }
}
