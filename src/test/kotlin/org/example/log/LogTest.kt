package org.example.log

import org.example.TestInstance
import org.example.assertPossiblyArrayEquals
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
            assertEquals(0, it.instance().append(nextValue()))
        }
    }.asStream()

    @TestFactory fun `append should sum offset`() = instances().map {
        dynamicTest(it.name) {
            val log = it.instance()
            val firstOffset = log.append(nextValue())
            val secondOffset = log.append(nextValue())

            assertTrue(firstOffset < secondOffset)
        }
    }.asStream()

    @TestFactory fun `single append should be readable`() = instances().map { case ->
        dynamicTest(case.name) {
            val log = case.instance()
            val expected = nextValue()
            log.append(expected)

            val items = log.useEntries { it.toList() }

            assertEquals(items.size, 1)

            val actual = items[0]
            assertPossiblyArrayEquals(expected, actual)
        }
    }.asStream()

    @TestFactory fun `multiple appends should be readable`() = instances().map { case ->
        dynamicTest(case.name) {

            multipleReadWriteCycle(case.instance()) { log, values -> values.forEach { log.append(it) }}

        }
    }.asStream()

    @TestFactory fun `atomic multiple appends (appendAll) should be readable`() = instances().map { case ->
        dynamicTest(case.name) {
            multipleReadWriteCycle(case.instance()) { log, values -> log.appendAll(values) }
        }
    }.asStream()

    fun multipleReadWriteCycle(log: Log<T>, append: (Log<T>, Sequence<T>) -> Unit) {

        val expectedList = sequence<T> { nextValue() }
                .take(50)
                .toList()

        append(log, expectedList.asSequence())

        val actualList = log.useEntries { it.toList() }

        assertEquals(expectedList.size, actualList.size)

        expectedList.zip(actualList).forEach {

            val expected = it.first
            val actual = it.second

            assertPossiblyArrayEquals(expected, actual)
        }
    }

}
