package org.example

import org.apache.commons.io.FileUtils
import java.io.Closeable
import java.nio.file.Files
import java.nio.file.Files.createTempDirectory
import java.nio.file.Files.createTempFile
import java.nio.file.Path
import java.util.*

class TestResources: Closeable {

    private val resources = Stack<Path>()

    fun allocateTempDir(prefix: String): Path = createTempDirectory(prefix)
            .also { resources.push(it) }

    fun allocateTempFile(prefix: String, suffix: String): Path = createTempFile(prefix, suffix)
            .let { resources.push(it) }

    override fun close() {
        while (resources.isNotEmpty()) {
            val resource = resources.pop()
            if (Files.isDirectory(resource)) {
                FileUtils.deleteDirectory(resource.toFile())
            } else {
                Files.deleteIfExists(resource)
            }
        }
    }
}
