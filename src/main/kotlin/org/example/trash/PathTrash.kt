package org.example.trash

import org.apache.commons.io.FileUtils
import java.nio.file.Path
import kotlin.io.path.deleteIfExists
import kotlin.io.path.isDirectory

object PathTrash: Trash<Path> {

    override fun mark(deleteMe: Path) {
        when {
            deleteMe.isDirectory() -> FileUtils.deleteDirectory(deleteMe.toFile())
            else -> deleteMe.deleteIfExists()
        }
    }
}