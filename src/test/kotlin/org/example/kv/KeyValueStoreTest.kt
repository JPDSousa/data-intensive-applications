package org.example.kv

import org.example.TestInstance
import org.example.assertPossiblyArrayEquals
import org.example.test
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.function.Executable

@Suppress("FunctionName")
interface KeyValueStoreTest<K, V> {

    fun instances(): Sequence<TestInstance<KeyValueStore<K, V>>>

    fun nextKey(): K

    fun nextValue(): V
    
    @TestFactory fun `absent key`() = instances().test { kv ->
        assertNull(kv.get(nextKey()))
    }

    @TestFactory fun `written value should be readable`() = instances().test { kv ->
        val key = nextKey()
        val expected = nextValue()

        kv.put(key, expected)
        assertPossiblyArrayEquals(expected, kv.get(key))
    }

    @TestFactory fun `multiple keys are isolated`() = instances().test { kv ->
        val entries = (0..5).associate { Pair(nextKey(), nextValue()) }

        kv.putAll(entries)

        assertAll(entries.map { GetAssertion(kv, it.key, it.value) })
    }

    @TestFactory fun `key update`() = instances().test { kv ->
        val key = nextKey()
        val old = nextValue()
        val new = nextValue()

        kv.put(key, old)
        kv.put(key, new)

        assertPossiblyArrayEquals(new, kv.get(key))
    }

    @TestFactory fun `deleted key becomes absent`() = instances().test { kv ->
        val key1 = nextKey()
        val value1 = nextValue()

        val key2 = nextKey()
        val value2 = nextValue()

        kv.put(key1, value1)
        kv.put(key2, value2)
        kv.delete(key1)
        assertNull(kv.get(key1))
        assertPossiblyArrayEquals(kv.get(key2), value2)
    }

}

internal class GetAssertion<K, V>(private val kv: KeyValueStore<K, V>,
                                  private val key: K,
                                  private val expected: V): Executable {

    override fun execute() {
        assertPossiblyArrayEquals(expected, kv.get(key))
    }
}
