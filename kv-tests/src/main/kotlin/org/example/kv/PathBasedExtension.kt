package org.example.kv

import org.junit.jupiter.api.extension.*
import java.nio.file.Files
import java.nio.file.Files.deleteIfExists
import java.nio.file.Path

abstract class PathBasedExtension<T>(private val clazz: Class<T>): ParameterResolver, BeforeEachCallback,
        AfterEachCallback {

    private val paths: MutableMap<String, Path> = mutableMapOf()

    abstract fun createParameter(path: Path): T

    override fun resolveParameter(parameterContext: ParameterContext?, extensionContext: ExtensionContext?):
            T? = createParameter(paths[extensionContext!!.tempFileName()]!!)

    override fun supportsParameter(parameterContext: ParameterContext?, extensionContext: ExtensionContext?): Boolean
            = parameterContext!!.parameter.type == clazz

    override fun beforeEach(context: ExtensionContext?) {

        paths[context!!.tempFileName()] = Files.createTempFile("text-kv-", ".txt")
    }

    override fun afterEach(context: ExtensionContext?) {

        paths.remove(context!!.tempFileName())?.let { deleteIfExists(it) }
    }

    private fun ExtensionContext.tempFileName(): String = this.displayName ?: "no-name"

}
