package org.example.log

import kotlinx.serialization.ExperimentalSerializationApi
import org.example.TestInstance
import org.example.TestResources
import org.example.encoder.Encoders
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.util.concurrent.atomic.AtomicLong

internal class LogEncoderTest: LogTest<String> {

    private val uniqueGenerator = AtomicLong()

    @ExperimentalSerializationApi
    override fun instances() = logs.stringEncodedInstances()

    override fun nextValue() = uniqueGenerator.getAndIncrement()
            .toString()

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
