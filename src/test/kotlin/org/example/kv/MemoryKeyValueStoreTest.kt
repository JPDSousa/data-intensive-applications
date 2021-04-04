package org.example.kv

import org.example.TestInstance
import java.util.concurrent.atomic.AtomicLong

internal class MemoryKeyValueStoreTest: KeyValueStoreTest<String, String> {

    private val uniqueGenerator = AtomicLong()

    override fun instances() = sequenceOf(
        TestInstance("Memory KV") {
            MemoryKeyValueStore<String, String>()
        }
    )

    override fun nextKey() = uniqueGenerator.getAndIncrement()
        .toString()

    override fun nextValue() = nextKey()
}
