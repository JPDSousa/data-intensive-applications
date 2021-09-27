package org.example.kv

import org.example.TestInstance
import org.example.TestResources
import org.example.log.LogFactory
import org.example.test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.TestInfo

interface LogKeyValueStoreFactoryTest<K, V> {

    fun instances(): Sequence<TestInstance<LogKeyValueStoreFactory<K, V>>>

    val resources: TestResources

    fun nextEntry(): Map.Entry<K, V>

    val logFactory: LogFactory<Map.Entry<K, V>>

    @TestFactory fun `create should load file content`(info: TestInfo) = instances().test(info) { factory ->
        val path = resources.allocateTempFile("log-", ".log")
        val expectedEntries = (1..100).asSequence().map { nextEntry() }
            .associate { Pair(it.key, it.value) }

        val log = logFactory.create(path)
        val keyValueStore = factory.createFromLog(log)
        keyValueStore.putAll(expectedEntries)

        val recoveredKV = factory.createFromLog(log)
        val actualEntries = recoveredKV.useEntries {
                entries -> entries.associate { Pair(it.key, it.value) }
        }

        assertEquals(expectedEntries, actualEntries)
    }
}