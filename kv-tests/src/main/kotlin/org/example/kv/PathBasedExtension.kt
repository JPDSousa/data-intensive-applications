package org.example.kv

import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.support.TypeBasedParameterResolver
import java.nio.file.Files
import java.nio.file.Files.deleteIfExists
import java.nio.file.Path

abstract class PathBasedExtension: TypeBasedParameterResolver<KeyValueStore>(), BeforeEachCallback, AfterEachCallback {

    private val paths: MutableMap<String, Path> = mutableMapOf()

    abstract fun createKeyValueStore(path: Path): KeyValueStore

    override fun resolveParameter(parameterContext: ParameterContext?, extensionContext: ExtensionContext?):
            KeyValueStore? = createKeyValueStore(paths[extensionContext!!.tempFileName()]!!)

    override fun beforeEach(context: ExtensionContext?) {

        paths[context!!.tempFileName()] = Files.createTempFile("text-kv-", ".txt")
    }

    override fun afterEach(context: ExtensionContext?) {

        paths.remove(context!!.tempFileName())?.let { deleteIfExists(it) }
    }

    private fun ExtensionContext.tempFileName(): String = this.displayName ?: "no-name"

}
