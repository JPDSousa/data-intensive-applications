package org.example.log

import org.example.TestInstance
import org.example.kv.LogTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

internal class CSVFileLogTest: LogTest {

    private var path: Path? = null

    @BeforeEach
    fun createFiles(@TempDir path: Path) {
        this.path = Files.createTempFile(path, "kv-single", "")
    }

    override fun instances() = sequenceOf(TestInstance("Single", CSVFileLog(this.path!!) as Log))

}
