package org.example.kv

import org.example.TestInstance
import org.example.assertPossiblyArrayEquals
import org.example.concepts.ImmutableDictionaryMixin
import org.example.test
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.api.function.Executable

@Suppress("FunctionName")
interface KeyValueStoreTest<K, V> {

    fun instances(): Sequence<TestInstance<KeyValueStore<K, V>>>

    fun nextKey(): K

    fun nextValue(): V
    
    @TestFactory fun `absent key`(info: TestInfo) = instances().test(info) { kv ->
        assertNull(kv[nextKey()])
    }

    @TestFactory fun `written value should be readable`(info: TestInfo) = instances().test(info) { kv ->
        val key = nextKey()
        val expected = nextValue()

        kv[key] = expected
        assertPossiblyArrayEquals(expected, kv[key])
    }

    @TestFactory fun `multiple keys are isolated`(info: TestInfo) = instances().test(info) { kv ->
        val entries = (0..5).associate { Pair(nextKey(), nextValue()) }

        kv.putAll(entries)

        assertAll(entries.map { GetAssertion(kv, it.key, it.value) })
    }

    @TestFactory fun `key update`(info: TestInfo) = instances().test(info) { kv ->
        val key = nextKey()
        val old = nextValue()
        val new = nextValue()

        kv[key] = old
        kv[key] = new

        assertPossiblyArrayEquals(new, kv[key])
    }

    @TestFactory fun `deleted key becomes absent`(info: TestInfo) = instances().test(info) { kv ->
        val key1 = nextKey()
        val value1 = nextValue()

        val key2 = nextKey()
        val value2 = nextValue()

        kv[key1] = value1
        kv[key2] = value2
        kv.delete(key1)
        assertNull(kv[key1])
        assertPossiblyArrayEquals(kv[key2], value2)
    }

}

internal class GetAssertion<K, V>(private val kv: ImmutableDictionaryMixin<K, V>,
                                  private val key: K,
                                  private val expected: V): Executable {

    override fun execute() {
        assertPossiblyArrayEquals(expected, kv[key])
    }
}
