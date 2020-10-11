package org.example.kv

import org.example.TestInstance

internal class MemoryKeyValueStoreTest: KeyValueStoreTest {

    override fun instances() = sequenceOf(
            TestInstance("Memory KV", MemoryKeyValueStore() as KeyValueStore)
    )
}
