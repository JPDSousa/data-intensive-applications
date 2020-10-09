package org.example.kv

import org.apache.commons.io.FileUtils.deleteDirectory
import org.junit.jupiter.api.extension.*
import java.nio.file.Files
import java.nio.file.Files.*
import java.nio.file.Path

abstract class PathBasedExtension<T>(private val clazz: Class<T>, private val isFile: Boolean = true):
        ParameterResolver, BeforeEachCallback, AfterEachCallback {

    private val paths: MutableMap<String, Path> = mutableMapOf()

    abstract fun createParameter(path: Path): T

    override fun resolveParameter(parameterContext: ParameterContext?, extensionContext: ExtensionContext?):
            T? = createParameter(paths[extensionContext!!.tempFileName()]!!)

    override fun supportsParameter(parameterContext: ParameterContext?, extensionContext: ExtensionContext?): Boolean
            = parameterContext!!.parameter.type == clazz

    override fun beforeEach(context: ExtensionContext?) {

        paths[context!!.tempFileName()] = if(isFile) createTempFile("file-kv-", ".txt")
        else createTempDirectory("dir-kv-")
    }

    override fun afterEach(context: ExtensionContext?) {

        paths.remove(context!!.tempFileName())
                ?.let { if (isDirectory(it)) deleteDirectory(it.toFile()) else deleteIfExists(it) }
    }

    private fun ExtensionContext.tempFileName(): String = this.displayName ?: "no-name"

}
