package org.example

import org.apache.commons.io.FileUtils.deleteDirectory
import java.io.Closeable
import java.nio.file.Files.createTempDirectory
import java.nio.file.Files.createTempFile
import java.nio.file.Path
import java.util.*
import kotlin.io.path.deleteIfExists
import kotlin.io.path.isDirectory

class TestResources: Closeable {

    private val resources = Stack<Path>()

    fun allocateTempDir(prefix: String): Path = createTempDirectory(prefix)
            .also { resources.push(it) }

    fun allocateTempFile(prefix: String, suffix: String): Path = createTempFile(prefix, suffix)
            .let { resources.push(it) }

    fun allocateTempLogFile(prefix: String = "log-") = allocateTempFile(prefix, ".log")

    override fun close() {
        while (resources.isNotEmpty()) {
            val resource = resources.pop()
            if (resource.isDirectory()) {
                deleteDirectory(resource.toFile())
            } else {
                resource.deleteIfExists()
            }
        }
    }
}
