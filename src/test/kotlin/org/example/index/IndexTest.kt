package org.example.index

import org.example.TestInstance
import org.example.log.Index
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import kotlin.streams.asStream

@Suppress("FunctionName")
interface IndexTest<K> {

    fun instances(): Sequence<TestInstance<Index<K>>>

    fun nextKey(): K

    @TestFactory fun `offsets are persisted`() = instances().map {
        dynamicTest(it.name) {
            val index = it.instance

            val key = nextKey()
            val expected = 1234L

            index.putOffset(key, expected)
            assertEquals(expected, index.getOffset(key))
        }
    }.asStream()

    @TestFactory fun `absent entry has no offset`() = instances().map {
        dynamicTest(it.name) {
            val index = it.instance

            assertNull(index.getOffset(nextKey()))
        }
    }.asStream()

    @TestFactory fun `reads do not delete entries`() = instances().map {
        dynamicTest(it.name) {
            val index = it.instance

            val key = nextKey()
            val expected = 1234L

            index.putOffset(key, expected)
            assertEquals(expected, index.getOffset(key))
            // second read asserts that the value is still there
            assertEquals(expected, index.getOffset(key))
        }
    }.asStream()

    @TestFactory fun `sequential writes act as updates`() = instances().map {
        dynamicTest(it.name) {
            val index = it.instance

            val key = nextKey()
            val expected = 4321L
            index.putOffset(key, 1234L)
            index.putOffset(key, 4321L)

            assertEquals(expected, index.getOffset(key))
        }
    }.asStream()

    @TestFactory fun `keys are isolated`() = instances().map {
        dynamicTest(it.name) {
            val index = it.instance

            val key1 = nextKey()
            val value1 = 1234L
            val key2 = nextKey()
            val value2 = 4321L
            index.putOffset(key1, value1)
            index.putOffset(key2, value2)

            assertEquals(value1, index.getOffset(key1))
            assertEquals(value2, index.getOffset(key2))
        }
    }.asStream()
}
