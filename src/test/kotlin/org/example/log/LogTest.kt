package org.example.log

import org.example.TestInstance
import org.example.assertPossiblyArrayEquals
import org.example.test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.TestInfo


@Suppress("FunctionName")
interface LogTest<T> {

    fun instances(): Sequence<TestInstance<Log<T>>>

    fun nextValue(): T

    @TestFactory fun `append on empty file should return 0`(info: TestInfo) = instances().test(info) { log ->
        val initialSize = log.size
        assertEquals(initialSize, log.append(nextValue()))
    }

    @TestFactory fun `append should sum offset`(info: TestInfo) = instances().test(info) { log ->
        val firstOffset = log.append(nextValue())
        val secondOffset = log.append(nextValue())

        assertTrue(firstOffset < secondOffset)
    }

    @TestFactory fun `single append should be readable`(info: TestInfo) = instances().test(info) { log ->

        val initialSize = log.useEntries { it.toList() }.size

        val expected = nextValue()
        log.append(expected)

        val items = log.useEntries { it.toList() }

        assertEquals(items.size, initialSize + 1)

        val actual = items.last()
        assertPossiblyArrayEquals(expected, actual)
    }

    @TestFactory fun `multiple appends should be readable`(info: TestInfo) = instances().test(info) { log ->
        multipleReadWriteCycle(log) { values -> values.map { log.append(it) }}
    }

    @TestFactory fun `atomic multiple appends (appendAll) should be readable`(info: TestInfo) = instances().test(info) { log ->
        multipleReadWriteCycle(log) { values -> log.appendAll(values) }
    }

    @TestFactory fun `empty appendAll should not change structure`(info: TestInfo) = instances().test(info) { log ->
        multipleReadWriteCycle(log, 0) { values -> log.appendAll(values) }
    }

    @TestFactory fun `clear removes all entries`(info: TestInfo) = instances().test(info) { log ->

        log.clear()
        assertEquals(0L, log.size)
        assertTrue(log.useEntries { it.toList() }.isEmpty())
    }

    fun multipleReadWriteCycle(log: Log<T>, countValues: Int = 50, append: (Sequence<T>) -> Sequence<Long>) {

        val initialSize = log.useEntries { it.count() }

        val expectedList = generateSequence { nextValue() }
                .take(countValues)
                .toList()

        val insertedOffsets = append(expectedList.asSequence()).toList()

        val actualValuesList = log.useEntriesWithOffset { it.toList() }

        assertEquals(expectedList.size, insertedOffsets.size)
        assertEquals(initialSize + expectedList.size, actualValuesList.size)

        val testItems = actualValuesList.subList(initialSize, actualValuesList.size)

        expectedList.zip(testItems).forEach {

            val expected = it.first
            val actual = it.second.entry

            assertPossiblyArrayEquals(expected, actual)
        }

        testItems.zip(insertedOffsets).forEach {

            val writeOffset = it.second
            val readOffset = it.first.offset

            assertEquals(writeOffset, readOffset)
        }
    }

}
