package org.example.kv

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable

interface KeyValueStoreTest {

    @Test fun `absent key`(kv: KeyValueStore) {

        assertNull(kv.get("absent"))
    }

    @Test fun `written value should be readable`(kv: KeyValueStore) {

        val key = "key"
        val expected = "value"

        kv.put(key, expected)
        assertEquals(expected, kv.get(key))
    }

    @Test fun `multiple keys are isolated`(kv: KeyValueStore) {

        val entries = (0..5).associate { Pair("key$it", "value$it") }

        kv.putAll(entries)

        assertAll(entries.map { GetAssertion(kv, it.key, it.value) })
    }

    @Test fun `key update`(kv: KeyValueStore) {

        val key = "key"
        val old = "value"
        val new = "value1"

        kv.put(key, old)
        kv.put(key, new)

        assertEquals(new, kv.get(key))
    }

}

internal class GetAssertion(private val kv: KeyValueStore,
                            private val key: String,
                            private val expected: String): Executable {

    override fun execute() {
        assertEquals(expected, kv.get(key))
    }
}
