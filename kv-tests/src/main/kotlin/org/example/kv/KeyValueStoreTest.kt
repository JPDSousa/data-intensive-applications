package org.example.kv

import org.example.TestInstance
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.function.Executable
import kotlin.streams.asStream

interface KeyValueStoreTest {

    fun instances(): Sequence<TestInstance<KeyValueStore>>
    
    @TestFactory fun `absent key`() = instances().map { 
        dynamicTest(it.name) {
            assertNull(it.instance.get("absent"))
        }
    }.asStream()

    @TestFactory fun `written value should be readable`() = instances().map {
        dynamicTest(it.name) {
            val kv = it.instance
            val key = "key"
            val expected = "value"

            kv.put(key, expected)
            assertEquals(expected, kv.get(key))
        }
    }.asStream()

    @TestFactory fun `multiple keys are isolated`() = instances().map {
        dynamicTest(it.name) {
            val kv = it.instance
            val entries = (0..5).associate { Pair("key$it", "value$it") }

            kv.putAll(entries)

            assertAll(entries.map { GetAssertion(kv, it.key, it.value) })
        }
    }.asStream()

    @TestFactory fun `key update`() = instances().map {
        dynamicTest(it.name) {

            val kv = it.instance
            val key = "key"
            val old = "value"
            val new = "value1"

            kv.put(key, old)
            kv.put(key, new)

            assertEquals(new, kv.get(key))
        }
    }.asStream()

}

internal class GetAssertion(private val kv: KeyValueStore,
                            private val key: String,
                            private val expected: String): Executable {

    override fun execute() {
        assertEquals(expected, kv.get(key))
    }
}
