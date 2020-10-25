package org.example.log

import org.example.TestInstance
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong

internal class BinaryLogTest: LogTest<ByteArray> {

    private var path: Path? = null
    private val uniqueGenerator = AtomicLong()

    @BeforeEach
    fun createFiles(@TempDir path: Path) {
        this.path = Files.createTempFile(path, "log-", "")
    }

    override fun instances() = sequenceOf(TestInstance("Line", BinaryLog(this.path!!) as Log<ByteArray>))

    override fun nextValue(): ByteArray = uniqueGenerator.getAndIncrement()
            .toString()
            .toByteArray()

}
