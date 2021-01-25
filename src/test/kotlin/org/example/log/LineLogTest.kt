package org.example.log

import org.example.TestResources
import org.example.encoder.Encoders
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import java.util.concurrent.atomic.AtomicLong
import kotlin.streams.asStream

internal class LineLogTest: LogTest<String> {

    private val uniqueId = AtomicLong()

    @TestFactory
    fun `entries should be partitioned by lines`() = instances().map { case ->
        dynamicTest(case.name) {

            val log = case.instance()
            val entries = (1..100).map { nextValue() }
            val expected = entries.joinToString("\n")

            entries.forEach { log.append(it) }

            val content = log.useEntries { it.joinToString("\n") }

            assertEquals(expected, content)
        }
    }.asStream()

    override fun instances() = logs.lineLogInstances()

    override fun nextValue() = uniqueId.getAndIncrement()
            .toString()

    companion object {

        @JvmStatic
        private val resources = TestResources()

        @JvmStatic
        private val logs = Logs(resources, LogFactories(Encoders()))

        @JvmStatic
        @AfterAll
        fun closeResources() {
            resources.close()
        }

    }

}
