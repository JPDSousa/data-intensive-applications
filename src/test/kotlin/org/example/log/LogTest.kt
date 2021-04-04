package org.example.log

import org.example.TestInstance
import org.example.assertPossiblyArrayEquals
import org.example.test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.TestFactory

@Suppress("FunctionName")
interface LogTest<T> {

    fun instances(): Sequence<TestInstance<Log<T>>>

    fun nextValue(): T

    @TestFactory fun `append on empty file should return 0`() = instances().test { log ->
        assertEquals(0, log.append(nextValue()))
    }

    @TestFactory fun `append should sum offset`() = instances().test { log ->
        val firstOffset = log.append(nextValue())
        val secondOffset = log.append(nextValue())

        assertTrue(firstOffset < secondOffset)
    }

    @TestFactory fun `single append should be readable`() = instances().test { log ->
        val expected = nextValue()
        log.append(expected)

        val items = log.useEntries { it.toList() }

        assertEquals(items.size, 1)

        val actual = items[0]
        assertPossiblyArrayEquals(expected, actual)
    }

    @TestFactory fun `multiple appends should be readable`() = instances().test { log ->
        multipleReadWriteCycle(log) { values -> values.forEach { log.append(it) }}
    }

    @TestFactory fun `atomic multiple appends (appendAll) should be readable`() = instances().test { log ->
        multipleReadWriteCycle(log) { values -> log.appendAll(values) }
    }

    fun multipleReadWriteCycle(log: Log<T>, append: (Sequence<T>) -> Unit) {

        val expectedList = sequence<T> { nextValue() }
                .take(50)
                .toList()

        append(expectedList.asSequence())

        val actualList = log.useEntries { it.toList() }

        assertEquals(expectedList.size, actualList.size)

        expectedList.zip(actualList).forEach {

            val expected = it.first
            val actual = it.second

            assertPossiblyArrayEquals(expected, actual)
        }
    }

}
