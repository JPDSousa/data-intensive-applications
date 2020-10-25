package org.example.log

import org.example.TestInstance
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import kotlin.streams.asStream

@Suppress("FunctionName")
interface LogTest<T> {

    fun instances(): Sequence<TestInstance<Log<T>>>

    fun nextValue(): T

    @TestFactory fun `append on empty file should return 0`() = instances().map {
        dynamicTest(it.name) {
            assertEquals(0, it.instance.append(nextValue()))
        }
    }.asStream()

    @TestFactory fun `append should sum offset`() = instances().map {
        dynamicTest(it.name) {
            val log = it.instance
            val firstOffset = log.append(nextValue())
            val secondOffset = log.append(nextValue())

            assertTrue(firstOffset < secondOffset)
        }
    }.asStream()

    @TestFactory fun `append should be readable`() = instances().map { case ->
        dynamicTest(case.name) {
            val log = case.instance
            val expected = nextValue()
            log.append(expected)

            log.useEntries {
                val items = it.toList()
                assertEquals(items.size, 1)

                val actual = items[0]
                if (expected is ByteArray && actual is ByteArray) {
                    assertArrayEquals(expected, actual)
                } else {
                    assertEquals(expected, actual)
                }
            }
        }
    }.asStream()

}
