package org.example.log

import org.example.TestInstance
import org.example.kv.LogTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong
import kotlin.streams.asStream

internal class LineLogTest: LogTest<String> {

    private var path: Path? = null
    private val uniqueId = AtomicLong()

    @BeforeEach
    fun createFiles(@TempDir path: Path) {
        this.path = Files.createTempFile(path, "log", "")
    }

    @TestFactory
    fun `entries should be partitioned by lines`() = instances().map { case ->
        DynamicTest.dynamicTest(case.name) {

            val log = case.instance
            val entries = (1..100).map { nextValue() }
            val expected = entries.joinToString("\n")

            entries.forEach { log.append(it) }

            val content = log.useEntries { it.joinToString("\n") }

            Assertions.assertEquals(expected, content)
        }
    }.asStream()

    override fun instances() = sequenceOf(TestInstance("Line", LineLog(this.path!!) as Log<String>))

    override fun nextValue() = uniqueId.getAndIncrement()
            .toString()

}
