package org.example.log

import mu.KotlinLogging
import org.example.TestInstance
import org.example.kv.LogTest
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.file.Files
import java.nio.file.Path

internal class SingleFileLogTest: LogTest {

    private var path: Path? = null

    @BeforeEach
    fun createFiles(@TempDir path: Path) {
        this.path = Files.createTempFile(path, "kv-single", "")
    }

    override fun instances() = sequenceOf(TestInstance("Single", SingleFileLog(this.path!!) as Log))

}
