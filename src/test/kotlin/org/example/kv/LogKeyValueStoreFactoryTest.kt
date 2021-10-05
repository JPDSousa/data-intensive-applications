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

    fun logFactories(): Sequence<TestInstance<LogFactory<Map.Entry<K, V>>>>

    @TestFactory fun `create should load file content`(info: TestInfo) = instances().flatMap { factory -> sequence {
        for (logFactory in logFactories()) {
            yield(TestInstance("${factory.name} ~ ${logFactory.name}") {
                Pair(factory.instance(), logFactory.instance())
            })
        }
    }}.test(info) { instancePair ->
        val factory = instancePair.first
        val logFactory = instancePair.second
        val path = resources.allocateTempFile("log-", ".log")
        val expectedEntries = (1..100).asSequence().map { nextEntry() }
            .associate { Pair(it.key, it.value) }

        val log = logFactory.create(path)
        val keyValueStore = factory.createFromPair(log)
        keyValueStore.putAll(expectedEntries)

        val recoveredKV = factory.createFromPair(log)
        val actualEntries = recoveredKV.useEntries {
                entries -> entries.associate { Pair(it.key, it.value) }
        }

        assertEquals(expectedEntries, actualEntries)
    }
}