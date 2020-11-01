package org.example

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals

fun assertPossiblyArrayEquals(expected: Any?, actual: Any?) {

    if (expected is ByteArray && actual is ByteArray) {
        assertArrayEquals(expected, actual)
    } else {
        assertEquals(expected, actual)
    }
}
