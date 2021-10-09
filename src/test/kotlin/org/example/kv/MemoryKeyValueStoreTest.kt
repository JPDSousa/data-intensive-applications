package org.example.kv

import org.example.TestInstance
import org.example.generator.StringGenerator
import java.util.concurrent.atomic.AtomicLong

internal class MemoryKeyValueStoreTest: AbstractIndexedKeyValueStoreTest<String, String>() {

    private val uniqueGenerator = AtomicLong()

    override fun instances() = sequenceOf(
        TestInstance("${MemoryKeyValueStore::class.simpleName}") {
            MemoryKeyValueStore<String, String>()
        }
    )

    override fun nextKey() = uniqueGenerator.getAndIncrement()
        .toString()

    override fun nextValue() = nextKey()

}
