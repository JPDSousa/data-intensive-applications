package org.example.kv

import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtensionContext
import java.nio.file.Files
import java.nio.file.Path

abstract class PathStoreExtension(private val isFile: Boolean = true): BeforeEachCallback, AfterEachCallback {

    override fun beforeEach(context: ExtensionContext?) {

        val path = when {
            isFile -> Files.createTempFile("file-kv-", ".txt")
            else -> Files.createTempDirectory("dir-kv-")
        }

        context!!.getStore().putPath(context, path)
    }

    override fun afterEach(context: ExtensionContext?) {

        context?.getStore()?.removePath(context)
    }

    private fun ExtensionContext.Store.putPath(context: ExtensionContext, path: Path)
            = put(context.tempFileName(), path)

    private fun ExtensionContext.Store.removePath(context: ExtensionContext)
            = this.remove(context.tempFileName(), Path::class.java)?.deleteIfExists()

    private fun Path.deleteIfExists() {
        if (Files.isDirectory(this)) {
            FileUtils.deleteDirectory(this.toFile())
        } else {
            Files.deleteIfExists(this)
        }
    }
}

fun ExtensionContext.getStore() = this.getStore(ExtensionContext.Namespace.GLOBAL)

fun ExtensionContext.Store.getPath(context: ExtensionContext)
        = this[context.tempFileName(), Path::class.java]!!

private fun ExtensionContext.tempFileName(): String = this.displayName ?: "no-name"
