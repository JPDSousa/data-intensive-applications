package org.example.log

import org.apache.commons.io.FileUtils
import org.example.TestInstance
import java.io.Closeable
import java.nio.file.Files
import java.nio.file.Path

class TextLogs: Closeable {

    private val singlePath = Files.createTempFile("single-", ".log")
    private val segmentedPath = Files.createTempDirectory("seg-")
    private val smallSegmentedPath = Files.createTempDirectory("seg-small-")

    fun instances() = sequenceOf<TestInstance<Log>>(
            TestInstance("Single File Log", SingleFileLog(singlePath)),
            TestInstance("Segmented Log", SegmentedLog(segmentedPath)),
            TestInstance("Small Segmented Log", SegmentedLog(smallSegmentedPath, 5))
    )

    override fun close() {
        singlePath.deleteIfExists()
        segmentedPath.deleteIfExists()
        smallSegmentedPath.deleteIfExists()
    }

    private fun Path.deleteIfExists() {
        if (Files.isDirectory(this)) {
            FileUtils.deleteDirectory(this.toFile())
        } else {
            Files.deleteIfExists(this)
        }
    }
}
