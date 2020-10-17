package org.example.log

import org.example.TestInstance
import java.io.Closeable
import java.nio.file.Files

class TextLogs: Closeable {

    private val csvPath = Files.createTempFile("log-", ".csv")

    fun instances() = sequenceOf(
            TestInstance("CSV Log", SingleFileLog(csvPath))
    )

    override fun close() {
        Files.deleteIfExists(this.csvPath)
    }

}
