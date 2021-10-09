package org.example.kv

import org.example.TestInstance
import org.example.TestResources
import org.example.assertPossiblyArrayEquals
import org.example.test
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.api.function.Executable

@Suppress("FunctionName")
interface KeyValueStoreTest<K, V> {

    val resources: TestResources

    fun instances(): Sequence<TestInstance<KeyValueStore<K, V>>>

    fun nextKey(): K

    fun nextValue(): V
    
    @TestFactory fun `absent key`(info: TestInfo) = instances().test(info) { kv ->
        assertNull(kv.get(nextKey()))
        resources.close()
    }

    @TestFactory fun `written value should be readable`(info: TestInfo) = instances().test(info) { kv ->
        val key = nextKey()
        val expected = nextValue()

        kv.put(key, expected)
        assertPossiblyArrayEquals(expected, kv.get(key))
        resources.close()
    }

    @TestFactory fun `multiple keys are isolated`(info: TestInfo) = instances().test(info) { kv ->
        val entries = (0..5).associate { Pair(nextKey(), nextValue()) }

        kv.putAll(entries)

        assertAll(entries.map { GetAssertion(kv, it.key, it.value) })
        resources.close()
    }

    @TestFactory fun `key update`(info: TestInfo) = instances().test(info) { kv ->
        val key = nextKey()
        val old = nextValue()
        val new = nextValue()

        kv.put(key, old)
        kv.put(key, new)

        assertPossiblyArrayEquals(new, kv.get(key))
        resources.close()
    }

    @TestFactory fun `deleted key becomes absent`(info: TestInfo) = instances().test(info) { kv ->
        val key1 = nextKey()
        val value1 = nextValue()

        val key2 = nextKey()
        val value2 = nextValue()

        kv.put(key1, value1)
        kv.put(key2, value2)
        kv.delete(key1)
        assertNull(kv.get(key1))
        assertPossiblyArrayEquals(kv.get(key2), value2)
        resources.close()
    }

}

internal class GetAssertion<K, V>(private val kv: KeyValueStore<K, V>,
                                  private val key: K,
                                  private val expected: V): Executable {

    override fun execute() {
        assertPossiblyArrayEquals(expected, kv.get(key))
    }
}
