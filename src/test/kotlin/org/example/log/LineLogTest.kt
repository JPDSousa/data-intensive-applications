package org.example.log

import kotlinx.serialization.ExperimentalSerializationApi
import org.example.ApplicationTest
import org.example.TestInstance
import org.example.TestResources
import org.example.generator.StringGenerator
import org.example.test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.TestInfo
import java.util.concurrent.atomic.AtomicLong

internal class LineLogTest: ApplicationTest(), LogTest<String> {

    private val valueIterator = stringGenerator.generate().iterator()

    override fun instances() = generator.generate()

    override fun nextValue() = when {
        valueIterator.hasNext() -> valueIterator.next()
        else -> throw NoSuchElementException("No more values")
    }

    @TestFactory
    fun `entries should be partitioned by lines`(info: TestInfo) = instances().test(info) { log ->
        entriesShouldBePartitionedByLines(log) { entries ->
            entries.forEach { log.append(it) }
        }
    }

    @TestFactory
    fun `entries should be partitioned by lines (appendAll(`(info: TestInfo) = instances().test(info) { log ->
        entriesShouldBePartitionedByLines(log) { entries ->
            log.appendAll(entries)
        }
    }

    private fun entriesShouldBePartitionedByLines(log: Log<String>, append: (Sequence<String>) -> Unit) {

        val entries = (1..100).map { nextValue() }
            .asSequence()
        val expected = entries.joinToString("\n")

        val initialSize = log.useEntries { it.count() }

        append(entries)

        val testContent = log.useEntries { it.drop(initialSize).joinToString("\n") }

        assertEquals(expected, testContent)
    }

    companion object {

        @JvmStatic
        private val stringGenerator: StringGenerator = application.koin.get()

        @JvmStatic
        private val generator: StringLogs = application.koin.get(lineLogQ)

    }

}

internal class LineLogFactoryTest: ApplicationTest(), LogFactoryTest<String> {

    @ExperimentalSerializationApi
    override fun instances(): Sequence<TestInstance<LogFactory<String>>> = logFactories.generate()

    override val resources: TestResources
        get() = application.koin.get()

    private val uniqueGenerator = AtomicLong()

    override fun nextValue(): String = uniqueGenerator.getAndIncrement()
        .toString()

    companion object {

        @JvmStatic
        private val logFactories: StringLogFactories = application.koin.get(lineLogQ)

    }
}
