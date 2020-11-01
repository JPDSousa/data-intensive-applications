package org.example.log

import org.example.TestInstance
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.util.concurrent.atomic.AtomicLong

internal class LogEncoderTest: LogTest<String> {

    private val uniqueGenerator = AtomicLong()

    override fun instances() = logs.stringEncodedInstances()

    override fun nextValue() = uniqueGenerator.getAndIncrement()
            .toString()

    companion object {

        @JvmStatic
        private val logs = Logs()

        @JvmStatic
        @AfterAll
        fun closeLogs() {
            logs.close()
        }

    }
}