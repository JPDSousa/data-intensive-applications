package org.example.log

import org.example.TestInstance
import org.example.kv.LogTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.util.concurrent.atomic.AtomicLong

internal class StringEncoderLogTest: LogTest<String> {

    private var logs : Logs? = null
    private val uniqueGenerator = AtomicLong()

    @BeforeEach
    fun createFiles() {
        logs = Logs()
    }

    @AfterEach
    fun deleteFiles() {
        logs!!.close()
    }

    override fun instances() = logs!!.binaryInstances().map {
        TestInstance("Encoder ~ ${it.name}", StringEncoderLog(it.instance) as Log<String>)
    }

    override fun nextValue() = uniqueGenerator.getAndIncrement()
            .toString()
}
