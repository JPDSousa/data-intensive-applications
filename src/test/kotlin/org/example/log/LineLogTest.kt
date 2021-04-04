package org.example.log

import kotlinx.serialization.ExperimentalSerializationApi
import org.example.TestInstance
import org.example.TestResources
import org.example.encoder.Encoders
import org.example.test
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestFactory
import java.util.concurrent.atomic.AtomicLong

internal class LineLogTest: LogTest<String> {

    private val uniqueId = AtomicLong()

    @TestFactory
    fun `entries should be partitioned by lines`() = instances().test { log ->
        val entries = (1..100).map { nextValue() }
        val expected = entries.joinToString("\n")

        entries.forEach { log.append(it) }

        val content = log.useEntries { it.joinToString("\n") }

        assertEquals(expected, content)
    }

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

internal class LineLogFactoryTest: LogFactoryTest<String> {

    @ExperimentalSerializationApi
    override fun instances(): Sequence<TestInstance<LogFactory<String>>> = logFactories
        .stringInstances()

    override val resources: TestResources
        get() = LineLogFactoryTest.resources

    private val uniqueGenerator = AtomicLong()

    override fun nextValue(): String = uniqueGenerator.getAndIncrement()
        .toString()

    companion object {

        @JvmStatic
        private val resources = TestResources()

        @JvmStatic
        private val logFactories = LogFactories(Encoders())

        @JvmStatic
        @AfterAll
        fun closeResources() {
            resources.close()
        }
    }
}
