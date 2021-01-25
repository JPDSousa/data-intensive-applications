package org.example.log

import org.example.TestResources
import org.example.encoder.Encoders
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
        private val resources = TestResources()

        @JvmStatic
        private val logs = Logs(resources, LogFactories(Encoders()))

        @JvmStatic
        @AfterAll
        fun closeResources() {
            resources.close()
        }

    }

}
