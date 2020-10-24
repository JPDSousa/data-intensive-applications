package org.example.log

import org.example.TestInstance
import java.io.Closeable
import java.nio.file.Files
import java.nio.file.Files.deleteIfExists

class Logs: Closeable {

    private val linePath = Files.createTempFile("log-", ".csv")
    private val binaryPath = Files.createTempFile("log-", ".bin")

    fun instances() = stringInstances() + binaryInstances()

    fun stringInstances() = sequenceOf(
            TestInstance("Line Log", LineLog(linePath))
    )

    fun binaryInstances() = sequenceOf(TestInstance("Binary Log", BinaryLog(binaryPath)))

    override fun close() {
        deleteIfExists(this.linePath)
        deleteIfExists(this.binaryPath)
    }

}
