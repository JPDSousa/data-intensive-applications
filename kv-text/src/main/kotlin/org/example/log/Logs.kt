package org.example.log

import org.example.TestInstance
import java.io.Closeable
import java.nio.file.Files
import java.nio.file.Files.deleteIfExists

class Logs: Closeable {

    private val linePath = Files.createTempFile("log-", ".csv")
    private val binaryPath = Files.createTempFile("log-", ".bin")
    private val factory = LogFactory()

    fun stringInstances() = sequenceOf(TestInstance("Line Log", LineLog(linePath))) + binaryInstances().map {
        TestInstance("Encoder ~ ${it.name}", factory.stringEncoder(it.instance) as Log<String>)
    }

    fun binaryInstances() = sequenceOf(TestInstance("Binary Log", BinaryLog(binaryPath)))

    override fun close() {
        deleteIfExists(this.linePath)
        deleteIfExists(this.binaryPath)
    }

}
