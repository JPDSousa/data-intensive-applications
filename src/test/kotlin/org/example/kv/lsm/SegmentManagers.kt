package org.example.kv.lsm

import org.example.TestGenerator

interface SegmentManagers<K, V>: TestGenerator<SegmentManager<K, V>>

interface StringStringSegmentManagers: SegmentManagers<String, String>
interface LongByteArraySegmentManagers: SegmentManagers<Long, ByteArray>

class DelegateStringString(private val delegate: TestGenerator<SegmentManager<String, String>>)
    : StringStringSegmentManagers, SegmentManagers<String, String>, TestGenerator<SegmentManager<String, String>> by delegate

class DelegateLongByteArray(private val delegate: TestGenerator<SegmentManager<Long, ByteArray>>)
    : LongByteArraySegmentManagers, SegmentManagers<Long, ByteArray>, TestGenerator<SegmentManager<Long, ByteArray>> by delegate
