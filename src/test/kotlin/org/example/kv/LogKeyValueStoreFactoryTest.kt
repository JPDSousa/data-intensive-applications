package org.example.kv

import io.kotest.core.spec.style.shouldSpec
import io.kotest.engine.spec.tempfile
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.PropTestConfig
import io.kotest.property.arbitrary.next
import io.kotest.property.checkAll
import org.example.DataEntry
import org.example.TestInstance
import org.example.log.LogFactory
import org.example.toMap

fun <K, V> logKeyValueStoreFactoryTests(
    gen: Gen<TestInstance<LogKeyValueStoreFactory<K, V>>>,
    logFactories: Gen<TestInstance<LogFactory<Map.Entry<K, V>>>>,
    entryGen: Arb<DataEntry<K, V>>,
    config: PropTestConfig = PropTestConfig(maxFailure = 3, iterations = 100),
) = shouldSpec {

    fun nextEntries(size: Int) = (1..size).asSequence()
        .map { entryGen.next() }
        .toMap()

    should("create should load file content") {
        checkAll(config, gen, logFactories) { lKVInstance, logFactoryInstance ->

            val logFactory = logFactoryInstance.instance()
            val factory = lKVInstance.instance()

            val expectedEntries = nextEntries(100)

            val log = tempfile().toPath()
                .let { logFactory.create(it) }

            val keyValueStore = factory.createFromPair(log)
            keyValueStore.putAll(expectedEntries)

            val actualEntries = factory.createFromPair(log)
                .loadToMemory()

            actualEntries shouldBe expectedEntries
        }
    }
}
