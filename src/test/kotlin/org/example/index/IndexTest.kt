package org.example.index

import org.example.TestInstance
import org.example.test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.TestFactory

@Suppress("FunctionName")
interface IndexTest<K> {

    fun instances(): Sequence<TestInstance<Index<K>>>

    fun nextKey(): K

    @TestFactory fun `offsets are persisted`() = instances().test { index ->
        val key = nextKey()
        val expected = 1234L

        index.putOffset(key, expected)
        assertEquals(expected, index.getOffset(key))
    }

    @TestFactory fun `absent entry has no offset`() = instances().test { index ->
        assertNull(index.getOffset(nextKey()))
    }

    @TestFactory fun `reads do not delete entries`() = instances().test { index ->
        val key = nextKey()
        val expected = 1234L

        index.putOffset(key, expected)
        assertEquals(expected, index.getOffset(key))
        // second read asserts that the value is still there
        assertEquals(expected, index.getOffset(key))
    }

    @TestFactory fun `sequential writes act as updates`() = instances().test { index ->
        val key = nextKey()
        val expected = 4321L
        index.putOffset(key, 1234L)
        index.putOffset(key, 4321L)

        assertEquals(expected, index.getOffset(key))
    }

    @TestFactory fun `keys are isolated`() = instances().test { index ->
        val key1 = nextKey()
        val value1 = 1234L
        val key2 = nextKey()
        val value2 = 4321L
        index.putOffset(key1, value1)
        index.putOffset(key2, value2)

        assertEquals(value1, index.getOffset(key1))
        assertEquals(value2, index.getOffset(key2))
    }
}
