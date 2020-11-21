package org.example.log

import org.junit.jupiter.api.AfterAll
import java.util.concurrent.atomic.AtomicLong

internal class BinaryLogTest: LogTest<ByteArray> {

    private val uniqueGenerator = AtomicLong()

    override fun instances() = logs.binaryInstances()

    override fun nextValue(): ByteArray = uniqueGenerator.getAndIncrement()
            .toString()
            .toByteArray()

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
