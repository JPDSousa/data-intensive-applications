package org.example.kv

import org.example.*
import org.example.log.LogFactory
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.TestInfo

interface LogKeyValueStoreFactoryTest<K, V> {

    fun instances(): Sequence<TestInstance<LogKeyValueStoreFactory<K, V>>>

    val resources: TestResources

    fun nextEntry(): DataEntry<K, V>

    fun nextEntries(size: Int) = (1..size).asSequence()
        .map { nextEntry() }
        .toMap()

    fun logFactories(): Sequence<TestInstance<LogFactory<Map.Entry<K, V>>>>

    @TestFactory fun `create should load file content`(info: TestInfo) = instances().flatMap { factory -> sequence {
        for (logFactory in logFactories()) {
            yield(TestInstance("$factory ~ $logFactory") {
                FactoryCreation(factory, logFactory)
            })
        }
    }}.test(info) { testInstance ->
        val expectedEntries = nextEntries(100)

        val log = resources.allocateTempLogFile()
            .let { testInstance.logFactory.create(it) }

        val factory = testInstance.kvFactory
        val keyValueStore = factory.createFromPair(log)
        keyValueStore.putAll(expectedEntries)

        val actualEntries = factory.createFromPair(log)
            .loadToMemory()

        assertEquals(expectedEntries, actualEntries)
    }
}

private data class FactoryCreation<K, V>(val kvFactory: LogKeyValueStoreFactory<K, V>,
                                         val logFactory: LogFactory<Map.Entry<K, V>>) {
    constructor(kvFactory: TestInstance<LogKeyValueStoreFactory<K, V>>,
                logFactory: TestInstance<LogFactory<Map.Entry<K, V>>>)
            : this(kvFactory.instance(), logFactory.instance())
}