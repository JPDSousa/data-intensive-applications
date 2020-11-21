package org.example.log

import org.example.TestInstance
import java.io.Closeable
import java.nio.file.Files.createTempFile
import java.nio.file.Files.deleteIfExists
import java.nio.file.Path
import java.util.*

class Logs: Closeable {

    private val resources = Stack<Path>()
    private val factory = LogFactory()

    @Suppress("USELESS_CAST")
    fun lineLogInstances() = createTempFile("log-", ".csv")
            .let { resources.push(it) }
            .let { LineLog(it) as Log<String> }
            .let { TestInstance("Line Log", it) }
            .let { sequenceOf(it) }

    fun stringEncodedInstances() = binaryInstances().map {
        TestInstance("Encoder ~ ${it.name}", factory.stringEncoder(it.instance) as Log<String>)
    }

    fun stringInstances() = lineLogInstances() + stringEncodedInstances()

    @Suppress("USELESS_CAST")
    fun binaryInstances() = createTempFile("log-", ".bin")
            .let { resources.push(it) }
            .let { BinaryLog(it) as Log<ByteArray> }
            .let { TestInstance("Binary Log", it) }
            .let { sequenceOf(it) }

    override fun close() {

        while (resources.isNotEmpty()) {
            deleteIfExists(resources.pop())
        }

    }

}
