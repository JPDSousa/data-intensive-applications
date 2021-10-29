package org.example.index

import org.example.TestInstance
import org.example.test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.TestInfo

@Suppress("FunctionName")
interface IndexTest<K> {

    fun instances(): Sequence<TestInstance<Index<K>>>

    fun nextKey(): K

    @TestFactory fun `offsets are persisted`(info: TestInfo) = instances().test(info) { index ->
        val key = nextKey()
        val expected = 1234L

        index[key] = expected
        assertEquals(expected, index[key])
    }

    @TestFactory fun `absent entry has no offset`(info: TestInfo) = instances().test(info) { index ->
        assertNull(index[nextKey()])
    }

    @TestFactory fun `reads do not delete entries`(info: TestInfo) = instances().test(info) { index ->
        val key = nextKey()
        val expected = 1234L

        index[key] = expected
        assertEquals(expected, index[key])
        // second read asserts that the value is still there
        assertEquals(expected, index[key])
    }

    @TestFactory fun `sequential writes act as updates`(info: TestInfo) = instances().test(info) { index ->
        val key = nextKey()
        val expected = 4321L
        index[key] = 1234L
        index[key] = 4321L

        assertEquals(expected, index[key])
    }

    @TestFactory fun `keys are isolated`(info: TestInfo) = instances().test(info) { index ->
        val key1 = nextKey()
        val value1 = 1234L
        val key2 = nextKey()
        val value2 = 4321L
        index[key1] = value1
        index[key2] = value2

        assertEquals(value1, index[key1])
        assertEquals(value2, index[key2])
    }
}
